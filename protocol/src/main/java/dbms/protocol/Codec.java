package dbms.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Сериализация/десериализация запросов и ответов.
 *
 * Формат — фиксированный, little-endian, без length-префикса (размер каждого фрейма заранее
 * известен и зашит в код):
 * <pre>
 *   Request  (17 байт): u8 op | u64 key | u64 value
 *   Response ( 9 байт): u8 status | u64 value
 * </pre>
 *
 */
public final class Codec {

    /** Размер сериализованного запроса в байтах. */
    public static final int REQUEST_SIZE = 1 + 8 + 8;
    /** Размер сериализованного ответа в байтах. */
    public static final int RESPONSE_SIZE = 1 + 8;

    private Codec() {}

    // ---- запись ----

    public static void writeRequest(Request r, OutputStream out) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(r.op().code());
        buf.putLong(r.key());
        buf.putLong(r.value());
        out.write(buf.array());
    }

    public static void writeResponse(Response r, OutputStream out) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(RESPONSE_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(r.status().code());
        buf.putLong(r.value());
        out.write(buf.array());
    }

    // ---- чтение ----

    /**
     * Читает запрос целиком. Возвращает {@code null}, если соединение закрылось до начала
     * следующего запроса (это нормальное завершение, не ошибка). При неполном чтении посередине
     * запроса бросает {@link EOFException}.
     */
    public static Request readRequest(InputStream in) throws IOException {
        byte[] buf = new byte[REQUEST_SIZE];
        int read = readFullyOrEof(in, buf);
        if (read == 0) {
            return null; // клиент закрыл соединение между запросами — это ок
        }
        if (read != REQUEST_SIZE) {
            throw new EOFException("неполный запрос: " + read + " из " + REQUEST_SIZE + " байт");
        }
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        byte opCode = bb.get();
        long key = bb.getLong();
        long value = bb.getLong();
        Op op = Op.fromCode(opCode);
        if (op == null) {
            throw new IOException("неизвестный код операции: " + opCode);
        }
        return new Request(op, key, value);
    }

    /**
     * Читает ответ целиком. Не возвращает null — после корректного запроса клиент обязан
     * получить ровно один ответ; обрыв соединения здесь — это ошибка.
     */
    public static Response readResponse(InputStream in) throws IOException {
        byte[] buf = new byte[RESPONSE_SIZE];
        int read = readFullyOrEof(in, buf);
        if (read != RESPONSE_SIZE) {
            throw new EOFException("неполный ответ: " + read + " из " + RESPONSE_SIZE + " байт");
        }
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        byte statusCode = bb.get();
        long value = bb.getLong();
        Status status = Status.fromCode(statusCode);
        if (status == null) {
            throw new IOException("неизвестный код статуса: " + statusCode);
        }
        return new Response(status, value);
    }

    // ---- варианты для DataInput/DataOutput----

    public static void writeRequest(Request r, DataOutput out) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(REQUEST_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.put(r.op().code());
        buf.putLong(r.key());
        buf.putLong(r.value());
        out.write(buf.array());
    }

    public static Request readRequest(DataInput in) throws IOException {
        byte[] buf = new byte[REQUEST_SIZE];
        in.readFully(buf);
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        byte opCode = bb.get();
        long key = bb.getLong();
        long value = bb.getLong();
        Op op = Op.fromCode(opCode);
        if (op == null) {
            throw new IOException("неизвестный код операции: " + opCode);
        }
        return new Request(op, key, value);
    }

    /**
     * Читает ровно {@code buf.length} байт из стрима. Возвращает фактически прочитанное количество.
     * Возвращает 0, если первый же {@code read} вернул EOF (используется чтобы отличить
     * нормальное закрытие соединения от обрыва посередине запроса).
     */
    private static int readFullyOrEof(InputStream in, byte[] buf) throws IOException {
        int total = 0;
        while (total < buf.length) {
            int n = in.read(buf, total, buf.length - total);
            if (n < 0) {
                return total;
            }
            total += n;
        }
        return total;
    }
}
