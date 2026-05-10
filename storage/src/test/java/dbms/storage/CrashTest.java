package dbms.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Интеграционный тест на durability-контракт:
 *   1. Форкаем дочернюю JVM, которая открывает {@link Storage} и в плотном цикле пишет put-ы,
 *      печатая в stdout по одной строке "OK k v" на каждый durable ACK.
 *   2. В родителе читаем эти ACK-и в oracle-карту.
 *   3. После случайной задержки шлём SIGKILL через {@link Process#destroyForcibly()}.
 *   4. Переоткрываем ту же директорию состояния в родителе и проверяем, что каждый ACK-нутый
 *      ключ возвращает то же значение, что и было ACK-нуто.
 *
 * Child никогда не регистрирует сигнал-хэндлер — это соответствует требованию задания «сервер
 * не должен обрабатывать сигнал».
 *
 * Гоняется несколько итераций с разными моментами kill-а, чтобы с заметной вероятностью
 * попасть в путь WAL-append, путь fsync, путь применения, путь чекпоинта и путь ротации
 * сегмента.
 */
class CrashTest {

    @Test
    void durabilityHoldsAcrossRandomKills(@TempDir Path root) throws Exception {
        long seed = Long.getLong("storage.test.crashSeed", System.nanoTime());
        Random rnd = new Random(seed);
        // Маленькие пороги, чтобы ротация и чекпоинт срабатывали часто в течение тех нескольких
        // сотен миллисекунд, что работает child.
        long maxSeg = 64L * WalRecord.SERIALIZED_SIZE;       // сегменты ~1.8 KiB
        long ckpt   = 256L * WalRecord.SERIALIZED_SIZE;       // чекпоинт каждые ~7 KiB

        int iterations = 5;
        for (int i = 0; i < iterations; i++) {
            Path dir = root.resolve("iter-" + i);
            Files.createDirectories(dir);
            long childSeed = rnd.nextLong();
            long killAfterMillis = 80 + rnd.nextInt(420); // 80..500 мс
            runOneIteration(dir, maxSeg, ckpt, childSeed, killAfterMillis, i, seed);
        }
    }

    private void runOneIteration(Path dir, long maxSeg, long ckpt,
                                 long childSeed, long killAfterMillis,
                                 int iter, long topSeed) throws Exception {
        ProcessBuilder pb = childProcess(dir, maxSeg, ckpt, childSeed);
        pb.redirectErrorStream(false);
        Process child = pb.start();

        // Поток-читатель копит ACK-и.
        Map<Long, Long> oracle = new LinkedHashMap<>();
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(killAfterMillis);

        Thread reader = new Thread(() -> {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("OK ")) {
                        String[] parts = line.split(" ");
                        long k = Long.parseLong(parts[1]);
                        long v = Long.parseLong(parts[2]);
                        synchronized (oracle) { oracle.put(k, v); }
                    }
                }
            } catch (IOException ignored) {
                // процесс убит — pipe закрыт
            }
        }, "crash-test-reader-" + iter);
        reader.setDaemon(true);
        reader.start();

        // Ждём kill-deadline и хотя бы 50 ACK-ов (чтобы не убить до выхода в стабильный режим).
        while (System.nanoTime() < deadlineNanos) {
            Thread.sleep(10);
        }
        long waitedExtra = 0;
        while (true) {
            int sz;
            synchronized (oracle) { sz = oracle.size(); }
            if (sz >= 50 || waitedExtra > 2000) break;
            Thread.sleep(20);
            waitedExtra += 20;
        }

        child.destroyForcibly();
        if (!child.waitFor(5, TimeUnit.SECONDS)) {
            fail("child JVM не умер после destroyForcibly (iter=" + iter
                    + ", topSeed=" + topSeed + ")");
        }
        reader.join(2000);

        Map<Long, Long> snapshot;
        synchronized (oracle) { snapshot = new HashMap<>(oracle); }
        assertThat(snapshot)
                .as("iter=%d topSeed=%d childSeed=%d killAfterMs=%d — child должен был успеть что-то ACK-нуть",
                        iter, topSeed, childSeed, killAfterMillis)
                .isNotEmpty();

        // Переоткрываем хранилище и проверяем durability каждого ACK-а.
        try (Storage s = Storage.open(dir, StorageConfig.forTesting(maxSeg, ckpt))) {
            for (Map.Entry<Long, Long> e : snapshot.entrySet()) {
                OptionalLong got = s.get(e.getKey());
                if (got.isEmpty() || got.getAsLong() != e.getValue()) {
                    String dump = dumpStateForKey(dir, e.getKey(), e.getValue(),
                            got.isEmpty() ? null : got.getAsLong());
                    fail("нарушение durability: key=" + e.getKey()
                            + " ACK-нутое value=" + e.getValue()
                            + " recovered=" + (got.isEmpty() ? "отсутствует" : got.getAsLong())
                            + " (iter=" + iter + ", topSeed=" + topSeed
                            + ", childSeed=" + childSeed + ", ackCount=" + snapshot.size() + ")\n"
                            + dump);
                }
            }
            // Storage должен быть пригоден для записи после recovery.
            s.put(-1L, -1L);
            assertThat(s.get(-1L)).isEqualTo(OptionalLong.of(-1L));
        }
    }

    /** Диагностика: дампим все WAL-записи по проблемному ключу + информацию о снапшоте. */
    private String dumpStateForKey(Path dir, long key, long expected, Long actual)
            throws IOException {
        StringBuilder sb = new StringBuilder();
        Snapshot.Loaded loaded = Snapshot.readIfPresent(dir);
        sb.append("snapshot.appliedLsn=")
                .append(loaded == null ? "<нет>" : loaded.appliedLsn())
                .append("  snapshot[").append(key).append("]=")
                .append(loaded == null ? "<нет>"
                        : (loaded.state().containsKey(key) ? loaded.state().get(key) : "<отсутствует>"))
                .append('\n');
        sb.append("WAL-сегменты:\n");
        Path walDir = dir.resolve("wal");
        for (Path p : Wal.listSegmentsSorted(walDir)) {
            sb.append("  ").append(p.getFileName()).append(" size=").append(Files.size(p)).append('\n');
        }
        sb.append("WAL-записи по ключу ").append(key).append(":\n");
        long startAfter = loaded == null ? 0 : loaded.appliedLsn();
        WalReader.scan(walDir, -1L, r -> {
            if (r.key() == key) {
                sb.append("  lsn=").append(r.lsn())
                        .append(" value=").append(r.value())
                        .append(r.lsn() <= startAfter ? "  (≤ snapshotLsn → пропущена)" : "  (проиграна)")
                        .append('\n');
            }
        });
        sb.append("ожидалось=").append(expected).append(" фактически=").append(actual);
        return sb.toString();
    }

    private ProcessBuilder childProcess(Path dir, long maxSeg, long ckpt, long seed) {
        String javaHome = System.getProperty("storage.test.javaHome",
                System.getProperty("java.home"));
        String classpath = System.getProperty("storage.test.classpath");
        if (classpath == null || classpath.isBlank()) {
            // Fallback на случай, если тест запущен вне Gradle.
            classpath = System.getProperty("java.class.path");
        }
        Path javaExe = Path.of(javaHome, "bin", "java");
        return new ProcessBuilder(
                javaExe.toString(),
                "-cp", classpath,
                "dbms.storage.crash.CrashChild",
                dir.toString(),
                Long.toString(maxSeg),
                Long.toString(ckpt),
                Long.toString(seed)
        );
    }
}
