package dbms.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * Формат на диске (little-endian):
 *   header  : magic(8 = "KVSNAP\0\1") | appliedLsn(u64) | count(u64) | headerCrc(u32)
 *   payload : count × { key(u64) | value(u64) }
 *   trailer : payloadCrc(u32)
 *
 * Атомарность: запись идёт в {@code snapshot.bin.tmp} и переименовывается через ATOMIC_MOVE.
 * tmp-файл fsync-ится до rename, директория fsync-ится после. Орфан {@code .tmp}, оставшийся
 * после крэша во время чекпоинта, игнорируется при чтении и зачищается на старте.
 */
public final class Snapshot {

    public static final String FILE_NAME = "snapshot.bin";
    public static final String TMP_NAME  = "snapshot.bin.tmp";

    private static final long MAGIC = 0x01_00_50_41_4E_53_56_4BL; // "KVSNAP\0\1" в little-endian
    private static final int  HEADER_CORE_SIZE = 8 + 8 + 8;       // magic + lsn + count
    private static final int  HEADER_SIZE      = HEADER_CORE_SIZE + 4;

    public record Loaded(long appliedLsn, Map<Long, Long> state) {}

    /** Атомарно записать снапшот {@code state} с {@code appliedLsn} в {@code dir}. */
    public static void write(Path dir, long appliedLsn, Map<Long, Long> state) throws IOException {
        Files.createDirectories(dir);
        Path tmp   = dir.resolve(TMP_NAME);
        Path final_ = dir.resolve(FILE_NAME);

        long count = state.size();
        long total = HEADER_SIZE + count * 16L + 4L;
        if (total > Integer.MAX_VALUE) {
            throw new IOException("снапшот не помещается в один буфер: " + total + " байт");
        }
        ByteBuffer buf = ByteBuffer.allocate((int) total).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(MAGIC);
        buf.putLong(appliedLsn);
        buf.putLong(count);
        ByteBuffer headerSlice = buf.duplicate();
        headerSlice.position(0).limit(HEADER_CORE_SIZE);
        CRC32C hcrc = new CRC32C();
        hcrc.update(headerSlice);
        buf.putInt((int) hcrc.getValue());

        int payloadStart = buf.position();
        for (Map.Entry<Long, Long> e : state.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue());
        }
        ByteBuffer payloadSlice = buf.duplicate();
        payloadSlice.position(payloadStart).limit(buf.position());
        CRC32C pcrc = new CRC32C();
        pcrc.update(payloadSlice);
        buf.putInt((int) pcrc.getValue());

        buf.flip();
        try (FileChannel ch = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
        Files.move(tmp, final_, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        // fsync директории, чтобы сам факт rename пережил краш.
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        } catch (IOException ignored) {
        }
    }

    /** Возвращает {@code null}, если снапшота нет или он повреждён. */
    public static Loaded readIfPresent(Path dir) throws IOException {
        Path final_ = dir.resolve(FILE_NAME);
        if (!Files.isRegularFile(final_)) {
            return null;
        }
        long size = Files.size(final_);
        if (size < HEADER_SIZE + 4) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate((int) size).order(ByteOrder.LITTLE_ENDIAN);
        try (FileChannel ch = FileChannel.open(final_, StandardOpenOption.READ)) {
            while (buf.hasRemaining()) {
                int read = ch.read(buf);
                if (read < 0) break;
            }
        }
        buf.flip();

        long magic = buf.getLong();
        if (magic != MAGIC) return null;
        long appliedLsn = buf.getLong();
        long count = buf.getLong();
        int storedHeaderCrc = buf.getInt();

        ByteBuffer headerSlice = buf.duplicate();
        headerSlice.position(0).limit(HEADER_CORE_SIZE);
        CRC32C hcrc = new CRC32C();
        hcrc.update(headerSlice);
        if ((int) hcrc.getValue() != storedHeaderCrc) return null;

        long expectedSize = HEADER_SIZE + count * 16L + 4L;
        if (expectedSize != size) return null;
        if (count < 0) return null;

        int payloadStart = buf.position();
        Map<Long, Long> state = new HashMap<>((int) Math.min(count, 1 << 20));
        for (long i = 0; i < count; i++) {
            long k = buf.getLong();
            long v = buf.getLong();
            state.put(k, v);
        }
        int storedPayloadCrc = buf.getInt();
        ByteBuffer payloadSlice = buf.duplicate();
        payloadSlice.position(payloadStart).limit(payloadStart + (int) (count * 16L));
        CRC32C pcrc = new CRC32C();
        pcrc.update(payloadSlice);
        if ((int) pcrc.getValue() != storedPayloadCrc) return null;

        return new Loaded(appliedLsn, state);
    }

    /** Очистка tmp-файла после крэша во время чекпоинта. */
    public static void cleanupTmp(Path dir) throws IOException {
        Files.deleteIfExists(dir.resolve(TMP_NAME));
    }

    private Snapshot() {}
}
