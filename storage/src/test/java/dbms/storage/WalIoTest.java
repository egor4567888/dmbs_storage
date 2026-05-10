package dbms.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class WalIoTest {

    @Test
    void appendThenScanReplaysInOrder(@TempDir Path tmp) throws IOException {
        Path walDir = tmp.resolve("wal");
        try (Wal w = Wal.open(walDir, 1, 1024 * 1024)) {
            for (long i = 1; i <= 100; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, i * 10));
            }
            w.fsync();
        }
        List<WalRecord> seen = new ArrayList<>();
        WalReader.ScanResult res = WalReader.scan(walDir, 0, seen::add);
        assertThat(res.lastValidLsn()).isEqualTo(100L);
        assertThat(seen).hasSize(100);
        assertThat(seen.get(0).lsn()).isEqualTo(1L);
        assertThat(seen.get(99).key()).isEqualTo(100L);
        assertThat(seen.get(99).value()).isEqualTo(1000L);
    }

    @Test
    void rotatesOnceSegmentExceedsMax(@TempDir Path tmp) throws IOException {
        Path walDir = tmp.resolve("wal");
        long maxSeg = 5L * WalRecord.SERIALIZED_SIZE; // 5 записей на сегмент
        try (Wal w = Wal.open(walDir, 1, maxSeg)) {
            for (long i = 1; i <= 13; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, 0));
            }
            w.fsync();
        }
        List<Path> segs = Wal.listSegmentsSorted(walDir);
        assertThat(segs).hasSize(3); // 5 + 5 + 3
        assertThat(Wal.startLsnOf(segs.get(0))).isEqualTo(1L);
        assertThat(Wal.startLsnOf(segs.get(1))).isEqualTo(6L);
        assertThat(Wal.startLsnOf(segs.get(2))).isEqualTo(11L);
    }

    @Test
    void scanStopsAtTornTailAndTruncationFixesIt(@TempDir Path tmp) throws IOException {
        Path walDir = tmp.resolve("wal");
        try (Wal w = Wal.open(walDir, 1, 1024 * 1024)) {
            for (long i = 1; i <= 5; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, i));
            }
            w.fsync();
        }
        // Дописываем мусор, чтобы сымитировать торн-тейл.
        Path seg = Wal.listSegmentsSorted(walDir).get(0);
        long sizeBefore = Files.size(seg);
        try (var ch = java.nio.channels.FileChannel.open(seg,
                java.nio.file.StandardOpenOption.WRITE, java.nio.file.StandardOpenOption.APPEND)) {
            byte[] garbage = new byte[7]; // обрывок записи
            for (int i = 0; i < garbage.length; i++) garbage[i] = (byte) 0xAB;
            ch.write(java.nio.ByteBuffer.wrap(garbage));
        }
        assertThat(Files.size(seg)).isEqualTo(sizeBefore + 7);

        List<WalRecord> seen = new ArrayList<>();
        WalReader.ScanResult res = WalReader.scan(walDir, 0, seen::add);
        assertThat(res.lastValidLsn()).isEqualTo(5L);
        assertThat(seen).hasSize(5);
        assertThat(res.tailValidBytes()).isEqualTo(sizeBefore);

        WalReader.truncateTail(res.tailSegmentPath(), res.tailValidBytes());
        assertThat(Files.size(seg)).isEqualTo(sizeBefore);
    }

    @Test
    void deleteSealedThroughDoesNotTouchActive(@TempDir Path tmp) throws IOException {
        Path walDir = tmp.resolve("wal");
        long maxSeg = 5L * WalRecord.SERIALIZED_SIZE;
        try (Wal w = Wal.open(walDir, 1, maxSeg)) {
            for (long i = 1; i <= 13; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, 0));
            }
            w.fsync();
            // сегменты: [1..5], [6..10], [11..13 активный]
            w.deleteSealedThrough(10);
            List<Path> remaining = Wal.listSegmentsSorted(walDir);
            assertThat(remaining).hasSize(1);
            assertThat(Wal.startLsnOf(remaining.get(0))).isEqualTo(11L);
        }
    }

    @Test
    void openResumesIntoExistingTailSegment(@TempDir Path tmp) throws IOException {
        Path walDir = tmp.resolve("wal");
        try (Wal w = Wal.open(walDir, 1, 1024 * 1024)) {
            for (long i = 1; i <= 3; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, i));
            }
            w.fsync();
        }
        try (Wal w = Wal.open(walDir, 4, 1024 * 1024)) {
            for (long i = 4; i <= 6; i++) {
                w.append(new WalRecord(i, WalRecord.OP_PUT, i, i));
            }
            w.fsync();
        }
        List<WalRecord> seen = new ArrayList<>();
        WalReader.ScanResult res = WalReader.scan(walDir, 0, seen::add);
        assertThat(res.lastValidLsn()).isEqualTo(6L);
        assertThat(seen).hasSize(6);
        assertThat(Wal.listSegmentsSorted(walDir)).hasSize(1);
    }
}
