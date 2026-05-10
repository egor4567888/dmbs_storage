package dbms.storage;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/**
 * Wire-формат одной WAL-записи (фиксированно 29 байт, little-endian):
 *   u64 lsn | u8 op | u64 key | u64 value | u32 crc32c
 *
 * crc32c покрывает байты от {@code lsn} до {@code value} включительно.
 * Этап 1 пишет только PUT; GET-ы не журналируются.
 */
public record WalRecord(long lsn, byte op, long key, long value) {

    public static final byte OP_PUT = 1;

    /** Размер полей, покрытых CRC. */
    static final int HEADER_SIZE = 8 + 1 + 8 + 8; // 25
    static final int CRC_SIZE = 4;
    public static final int SERIALIZED_SIZE = HEADER_SIZE + CRC_SIZE; // 29

    public void writeTo(ByteBuffer buf) {
        if (buf.order() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalArgumentException("буфер должен быть в порядке little-endian");
        }
        if (buf.remaining() < SERIALIZED_SIZE) {
            throw new IllegalArgumentException("в буфере " + buf.remaining()
                    + " байт, нужно " + SERIALIZED_SIZE);
        }
        int start = buf.position();
        buf.putLong(lsn);
        buf.put(op);
        buf.putLong(key);
        buf.putLong(value);
        ByteBuffer hdr = buf.duplicate();
        hdr.position(start);
        hdr.limit(buf.position());
        CRC32C crc = new CRC32C();
        crc.update(hdr);
        buf.putInt((int) crc.getValue());
    }

    /**
     * Парсит запись из буфера. Возвращает {@code null}, если CRC не сходится (признак торн-райта).
     * Позиция буфера в любом случае продвигается на размер записи.
     */
    public static WalRecord readFrom(ByteBuffer buf) {
        if (buf.order() != ByteOrder.LITTLE_ENDIAN) {
            throw new IllegalArgumentException("буфер должен быть в порядке little-endian");
        }
        if (buf.remaining() < SERIALIZED_SIZE) {
            throw new IllegalArgumentException("в буфере " + buf.remaining()
                    + " байт, нужно " + SERIALIZED_SIZE);
        }
        int start = buf.position();
        long lsn = buf.getLong();
        byte op = buf.get();
        long key = buf.getLong();
        long value = buf.getLong();
        int storedCrc = buf.getInt();

        ByteBuffer hdr = buf.duplicate();
        hdr.position(start);
        hdr.limit(start + HEADER_SIZE);
        CRC32C crc = new CRC32C();
        crc.update(hdr);
        if ((int) crc.getValue() != storedCrc) {
            return null;
        }
        return new WalRecord(lsn, op, key, value);
    }
}
