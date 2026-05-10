package dbms.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Встраиваемое durable KV-хранилище. K = V = {@code long} (трактуются как битовый паттерн u64).
 *
 * Durability — аналог PostgreSQL {@code wal_level=minimal}:
 *   1. {@link #put} кладёт запрос в очередь и блокируется.
 *   2. Commit-поток группирует ожидающие put-ы, дописывает их в WAL, делает fsync, затем
 *      применяет в карту в памяти и завершает future-ы. К моменту возврата {@code put} запись
 *      уже на диске.
 *   3. Когда WAL вырастает за {@link StorageConfig#checkpointWalBytesThreshold()}, commit-поток
 *      пишет снапшот и сносит запечатанные WAL-сегменты, которые им полностью покрыты.
 *
 * Recovery в {@link #open}: загрузить снапшот при наличии, проиграть WAL после его LSN, усечь
 * торн-тейл, продолжить дозапись.
 *
 * Важно: commit-поток НИКОГДА не interrupt-ится. {@link java.nio.channels.FileChannel}
 * закрывает сам себя, если его поток получит interrupt посреди I/O — это сломало бы durability.
 * Остановка сигналится через volatile-флаг {@code closed}, а сам поток опрашивает очередь
 * с коротким таймаутом.
 */
public final class Storage implements AutoCloseable {

    private final Path stateDir;
    private final StorageConfig cfg;

    private final KvState state;
    private final Wal wal;

    private final BlockingQueue<PendingPut> queue;
    private final Thread commitThread;

    private volatile boolean closed;
    private volatile Throwable fatal;
    private long lastAppliedLsn;

    private Storage(Path stateDir, StorageConfig cfg,
                    KvState state, Wal wal, long lastAppliedLsn) {
        this.stateDir = stateDir;
        this.cfg      = cfg;
        this.state    = state;
        this.wal      = wal;
        this.lastAppliedLsn = lastAppliedLsn;
        this.queue    = new ArrayBlockingQueue<>(Math.max(1024, cfg.commitBatchMax() * 4));
        this.commitThread = new Thread(this::commitLoop, "storage-commit");
        this.commitThread.setDaemon(true);
    }

    /** Открыть или создать хранилище в {@code stateDir}. Внутри выполняется crash recovery. */
    public static Storage open(Path stateDir, StorageConfig cfg) throws IOException {
        Files.createDirectories(stateDir);
        Path walDir = stateDir.resolve("wal");
        Files.createDirectories(walDir);

        Snapshot.cleanupTmp(stateDir);
        Snapshot.Loaded loaded = Snapshot.readIfPresent(stateDir);
        long appliedLsn = loaded == null ? 0 : loaded.appliedLsn();
        Map<Long, Long> initial = loaded == null ? Map.of() : loaded.state();
        KvState kv = new KvState(initial);

        WalReader.ScanResult scan = WalReader.scan(walDir, appliedLsn, r -> {
            if (r.op() == WalRecord.OP_PUT) {
                kv.put(r.key(), r.value());
            } else {
                throw new IOException("неизвестный код op в WAL: " + r.op() + " на lsn " + r.lsn());
            }
        });

        if (scan.tailSegmentPath() != null) {
            WalReader.truncateTail(scan.tailSegmentPath(), scan.tailValidBytes());
            WalReader.deleteSegmentsAfter(walDir, scan.tailSegmentPath());
        }

        long lastLsn = scan.lastValidLsn();
        Wal wal = Wal.open(walDir, lastLsn + 1, cfg.maxSegmentBytes());
        Storage s = new Storage(stateDir, cfg, kv, wal, lastLsn);
        s.commitThread.start();
        return s;
    }

    public OptionalLong get(long key) {
        ensureOpen();
        return state.get(key);
    }

    /** Блокируется, пока put не станет durable на диске. */
    public void put(long key, long value) throws IOException {
        ensureOpen();
        PendingPut p = new PendingPut(key, value, new CompletableFuture<>());
        try {
            queue.put(p);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupt во время постановки put в очередь", e);
        }

        if (closed && !p.ack.isDone()) {
            p.ack.completeExceptionally(new IOException("storage закрыт"));
        }
        try {
            p.ack.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupt во время ожидания commit", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) throw io;
            throw new IOException(cause);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        try {
            commitThread.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        List<PendingPut> drained = new ArrayList<>();
        queue.drainTo(drained);
        IOException reason = new IOException("storage закрыт");
        for (PendingPut p : drained) {
            p.ack.completeExceptionally(reason);
        }
        wal.close();
    }

    public long lastAppliedLsn() {
        return lastAppliedLsn;
    }

    public Path stateDir() {
        return stateDir;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("storage закрыт");
        }
        if (fatal != null) {
            throw new IllegalStateException("storage в fatal-состоянии", fatal);
        }
    }

    private void commitLoop() {
        List<PendingPut> batch = new ArrayList<>(cfg.commitBatchMax());
        while (true) {
            batch.clear();
            if (closed && queue.isEmpty()) return;
            PendingPut head;
            try {
                head = queue.poll(50, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted(); 
                continue;
            }
            if (head == null) continue;
            batch.add(head);
            queue.drainTo(batch, cfg.commitBatchMax() - 1);

            try {
                commitBatch(batch);
            } catch (IOException e) {
                fatal = e;
                for (PendingPut p : batch) {
                    p.ack.completeExceptionally(e);
                }
                return;
            }
        }
    }

    private void commitBatch(List<PendingPut> batch) throws IOException {
        long lsn = lastAppliedLsn + 1;
        for (PendingPut p : batch) {
            wal.append(new WalRecord(lsn++, WalRecord.OP_PUT, p.key, p.value));
        }
        wal.fsync();
        long lastInBatch = lsn - 1;

        for (PendingPut p : batch) {
            state.put(p.key, p.value);
        }
        lastAppliedLsn = lastInBatch;

        for (PendingPut p : batch) {
            p.ack.complete(null);
        }

        if (wal.totalBytes() > cfg.checkpointWalBytesThreshold()) {
            checkpoint();
        }
    }

    private void checkpoint() throws IOException {
        long applied = lastAppliedLsn;
        Map<Long, Long> copy = state.snapshotCopy();
        Snapshot.write(stateDir, applied, copy);
        wal.deleteSealedThrough(applied);
    }

    private record PendingPut(long key, long value, CompletableFuture<Void> ack) {}

    /** Тестовый хук: принудительный синхронный чекпоинт от лица commit-потока. */
    void checkpointForTest() {
        try {
            checkpoint();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
