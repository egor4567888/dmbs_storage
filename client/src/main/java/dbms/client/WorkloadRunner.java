package dbms.client;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Цикл генерации нагрузки на сервер.
 *
 * На каждой итерации: случайный ключ из заданного диапазона, случайно GET или PUT (50/50). При
 * OK — обновляем локальную реплику; при GET — сверяем ответ. При обрыве/UNAVAILABLE — backoff
 * с экспоненциальным ростом (с верхней границей и небольшим джиттером), затем переоткрываем
 * соединение и идём дальше.
 *
 * Поведение при ошибках:
 *  - UnavailableException на PUT: ключ как был. Backoff и реконнект.
 *  - UnavailableException на GET: backoff и реконнект (никаких локальных изменений).
 *  - IOException на PUT: ключ помечается как unknown — следующий GET его уточнит.
 *  - IOException на GET: backoff и реконнект (без изменения реплики).
 *
 * Класс рассчитан на запуск в отдельном потоке и остановку через volatile-флаг.
 */
public final class WorkloadRunner implements Runnable {

    private final int port;
    private final long keyRangeStart;
    private final long keyRangeSize;
    private final Random rng;
    private final LocalReplica replica = new LocalReplica();
    private final AtomicLong opsCompleted = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private volatile boolean stop;
    private volatile Throwable terminalError;

    /** Параметры backoff в миллисекундах. */
    private static final long BACKOFF_INITIAL_MS = 5;
    private static final long BACKOFF_MAX_MS = 500;

    /**
     * Простая форма для одного клиента: ключи равномерно из {@code [0, keySpaceSize)}.
     */
    public WorkloadRunner(int port, long keySpaceSize, long seed) {
        this(port, 0L, keySpaceSize, seed);
    }

    /**
     * Расширенная форма: ключи равномерно из {@code [keyRangeStart, keyRangeStart + keyRangeSize)}.
     * Используется в интеграционном chaos-тесте, чтобы дать каждому клиенту свой непересекающийся
     * диапазон — иначе клиенты затаптывают друг другу записи и верификация локальной реплики
     * становится бессмысленной.
     */
    public WorkloadRunner(int port, long keyRangeStart, long keyRangeSize, long seed) {
        if (keyRangeSize <= 0) {
            throw new IllegalArgumentException("keyRangeSize must be > 0");
        }
        this.port = port;
        this.keyRangeStart = keyRangeStart;
        this.keyRangeSize = keyRangeSize;
        this.rng = new Random(seed);
    }

    public void stop() {
        stop = true;
    }

    public LocalReplica replica() {
        return replica;
    }

    public long opsCompleted() {
        return opsCompleted.get();
    }

    public long reconnects() {
        return reconnects.get();
    }

    public Throwable terminalError() {
        return terminalError;
    }

    @Override
    public void run() {
        try {
            mainLoop();
        } catch (Throwable t) {
            terminalError = t;
        }
    }

    private void mainLoop() throws InterruptedException {
        long backoffMs = BACKOFF_INITIAL_MS;
        while (!stop) {
            try (KvClient client = KvClient.connect(port)) {
                reconnects.incrementAndGet();
                backoffMs = BACKOFF_INITIAL_MS; 
                while (!stop) {
                    if (!doOneOp(client)) {
                        break;
                    }
                }
            } catch (IOException connectError) {
            }
            if (stop) return;
            Thread.sleep(backoffWithJitter(backoffMs));
            backoffMs = Math.min(BACKOFF_MAX_MS, backoffMs * 2);
        }
    }

    /**
     * Одна итерация работы: одна операция. Возвращает true, если соединение можно переиспользовать
     * для следующей итерации; false — если соединение порвано/UNAVAILABLE и нужен реконнект.
     */
    private boolean doOneOp(KvClient client) {
        long key = keyRangeStart + (Math.abs(rng.nextLong()) % keyRangeSize);
        boolean isPut = rng.nextBoolean();
        try {
            if (isPut) {
                long value = rng.nextLong();
                client.put(key, value);
                replica.recordPutOk(key, value);
            } else {
                OptionalLong v = client.get(key);
                replica.verifyGet(key, v);
            }
            opsCompleted.incrementAndGet();
            return true;
        } catch (UnavailableException e) {

            if (isPut) {
                replica.recordPutRejected(key);
            }
            return false;
        } catch (IOException e) {
            if (isPut) {
                replica.recordPutUncertain(key);
            }
            return false;
        }
    }

    private long backoffWithJitter(long baseMs) {
        // ±25% джиттера — чтобы клиенты не синхронизировались.
        long jitter = (long) ((rng.nextDouble() - 0.5) * 0.5 * baseMs);
        return Math.max(1, baseMs + jitter);
    }
}
