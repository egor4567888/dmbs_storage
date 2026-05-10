package dbms.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class StorageTest {

    @Test
    void getReturnsEmptyWhenAbsent(@TempDir Path dir) throws IOException {
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            assertThat(s.get(123L)).isEqualTo(OptionalLong.empty());
        }
    }

    @Test
    void putThenGet(@TempDir Path dir) throws IOException {
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            s.put(1L, 100L);
            s.put(2L, 200L);
            assertThat(s.get(1L)).isEqualTo(OptionalLong.of(100L));
            assertThat(s.get(2L)).isEqualTo(OptionalLong.of(200L));
            assertThat(s.get(3L)).isEqualTo(OptionalLong.empty());
        }
    }

    @Test
    void closeAndReopenPreservesData(@TempDir Path dir) throws IOException {
        Map<Long, Long> oracle = new HashMap<>();
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            for (long i = 1; i <= 50; i++) {
                long v = i * 31;
                s.put(i, v);
                oracle.put(i, v);
            }
        }
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertThat(s.get(e.getKey())).isEqualTo(OptionalLong.of(e.getValue()));
            }
        }
    }

    @Test
    void overwriteSemantics(@TempDir Path dir) throws IOException {
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            s.put(7L, 1L);
            s.put(7L, 2L);
            s.put(7L, 3L);
            assertThat(s.get(7L)).isEqualTo(OptionalLong.of(3L));
        }
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            assertThat(s.get(7L)).isEqualTo(OptionalLong.of(3L));
        }
    }

    @Test
    void checkpointRotatesAndPrunesWal(@TempDir Path dir) throws IOException {
        // маленькие пороги: 4 записи на сегмент, чекпоинт каждые ~10 записей
        long maxSeg = 4L * WalRecord.SERIALIZED_SIZE;
        long ckptThreshold = 10L * WalRecord.SERIALIZED_SIZE;
        StorageConfig cfg = StorageConfig.forTesting(maxSeg, ckptThreshold);

        Map<Long, Long> oracle = new HashMap<>();
        try (Storage s = Storage.open(dir, cfg)) {
            Random r = new Random(1);
            for (int i = 0; i < 500; i++) {
                long k = r.nextLong(50);
                long v = r.nextLong();
                s.put(k, v);
                oracle.put(k, v);
            }
        }

        // После многих put-ов и чекпоинтов снапшот должен появиться.
        assertThat(Files.exists(dir.resolve("snapshot.bin"))).isTrue();

        try (Storage s = Storage.open(dir, cfg)) {
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertThat(s.get(e.getKey()))
                        .as("key=%d", e.getKey())
                        .isEqualTo(OptionalLong.of(e.getValue()));
            }
        }
    }

    @Test
    void recoveryAcrossTornTail(@TempDir Path dir) throws IOException {
        Map<Long, Long> oracle = new HashMap<>();
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            for (long i = 1; i <= 20; i++) {
                s.put(i, i * 11);
                oracle.put(i, i * 11);
            }
        }
        // Дописываем 13 байт мусора в активный сегмент, чтобы сымитировать торн-тейл.
        Path walDir = dir.resolve("wal");
        Path seg = Wal.listSegmentsSorted(walDir).get(0);
        long sizeBefore = Files.size(seg);
        try (var ch = java.nio.channels.FileChannel.open(seg,
                java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.APPEND)) {
            ch.write(java.nio.ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 0xa, 0xb, 0xc, 0xd}));
        }
        assertThat(Files.size(seg)).isGreaterThan(sizeBefore);

        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            // После open торн-тейл должен быть усечён.
            assertThat(Files.size(seg)).isEqualTo(sizeBefore);
            for (Map.Entry<Long, Long> e : oracle.entrySet()) {
                assertThat(s.get(e.getKey())).isEqualTo(OptionalLong.of(e.getValue()));
            }
            // Storage пригоден для новых записей.
            s.put(999L, 999L);
            assertThat(s.get(999L)).isEqualTo(OptionalLong.of(999L));
        }
    }

    @Test
    void concurrentWritersAllAcksAreDurable(@TempDir Path dir) throws Exception {
        int writers = 8;
        int perWriter = 250;
        Map<Long, Long> expected = new HashMap<>();
        for (int w = 0; w < writers; w++) {
            for (int i = 0; i < perWriter; i++) {
                expected.put((long) (w * perWriter + i), (long) (w * 100_000 + i));
            }
        }
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            Thread[] threads = new Thread[writers];
            for (int w = 0; w < writers; w++) {
                final int writerId = w;
                threads[w] = new Thread(() -> {
                    try {
                        for (int i = 0; i < perWriter; i++) {
                            s.put(writerId * perWriter + i, writerId * 100_000 + i);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[w].start();
            }
            for (Thread t : threads) t.join();
        }
        try (Storage s = Storage.open(dir, StorageConfig.defaults())) {
            for (Map.Entry<Long, Long> e : expected.entrySet()) {
                assertThat(s.get(e.getKey())).isEqualTo(OptionalLong.of(e.getValue()));
            }
        }
    }
}
