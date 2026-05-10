package dbms.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Read-only сканер для recovery. Идёт по WAL-сегментам в порядке LSN и останавливается на первой
 * записи, которая не валидна (CRC не сошлась, дыра в LSN, короткое чтение). Сообщает позицию,
 * до которой нужно усечь активный сегмент, чтобы будущие append-ы шли с чистого места.
 */
public final class WalReader {

    /** Результат сканирования при recovery. */
    public record ScanResult(
            long lastValidLsn,           // 0, если ни одной валидной записи не было
            Path tailSegmentPath,        // последний сегмент с записями (или тот, что нужно усечь)
            long tailValidBytes          // размер, до которого надо усечь хвостовой сегмент
    ) {}

    @FunctionalInterface
    public interface RecordSink {
        void accept(WalRecord r) throws IOException;
    }

    /**
     * Проиграть в {@code sink} записи с {@code lsn > startAfterLsn}. Останавливается на первой
     * невалидной записи (несовпавшая CRC, короткое чтение, либо LSN не равен предыдущему+1
     * в этом же сегменте).
     */
    public static ScanResult scan(Path walDir, long startAfterLsn, RecordSink sink)
            throws IOException {
        List<Path> segments = Wal.listSegmentsSorted(walDir);
        long lastValidLsn = startAfterLsn;
        Path tailPath = null;
        long tailValidBytes = 0;

        outer:
        for (int i = 0; i < segments.size(); i++) {
            Path seg = segments.get(i);
            long segStart = Wal.startLsnOf(seg);
            tailPath = seg;
            tailValidBytes = 0;

            try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
                long size = ch.size();
                ByteBuffer buf = ByteBuffer.allocate(WalRecord.SERIALIZED_SIZE)
                        .order(ByteOrder.LITTLE_ENDIAN);
                long expectedLsn = segStart;
                long pos = 0;
                while (pos + WalRecord.SERIALIZED_SIZE <= size) {
                    buf.clear();
                    int read = ch.read(buf, pos);
                    if (read < WalRecord.SERIALIZED_SIZE) {
                        break;
                    }
                    buf.flip();
                    WalRecord r = WalRecord.readFrom(buf);
                    if (r == null) {
                        break;
                    }
                    if (r.lsn() != expectedLsn) {
                        break;
                    }
                    if (r.lsn() > startAfterLsn) {
                        sink.accept(r);
                    }
                    pos += WalRecord.SERIALIZED_SIZE;
                    tailValidBytes = pos;
                    lastValidLsn = r.lsn();
                    expectedLsn++;
                }
                if (tailValidBytes != size) {
                    break outer;
                }
            }
        }
        return new ScanResult(lastValidLsn, tailPath, tailValidBytes);
    }

    /** Усечь хвостовой сегмент до {@code validBytes}, убрав мусор от торн-райта. */
    public static void truncateTail(Path segment, long validBytes) throws IOException {
        if (segment == null) return;
        try (FileChannel ch = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            if (ch.size() != validBytes) {
                ch.truncate(validBytes);
                ch.force(true);
            }
        }
    }

    /** Удалить все сегменты строго после {@code keep} (используется, когда scan остановился посредине). */
    public static void deleteSegmentsAfter(Path walDir, Path keep) throws IOException {
        if (keep == null) return;
        long keepStart = Wal.startLsnOf(keep);
        for (Path p : Wal.listSegmentsSorted(walDir)) {
            if (Wal.startLsnOf(p) > keepStart) {
                Files.deleteIfExists(p);
            }
        }
    }

    private WalReader() {}
}
