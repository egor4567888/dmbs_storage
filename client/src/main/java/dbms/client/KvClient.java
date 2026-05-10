package dbms.client;

import dbms.protocol.Codec;
import dbms.protocol.Request;
import dbms.protocol.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.OptionalLong;

/**
 * Низкоуровневая обёртка над одним TCP-соединением к серверу. Не делает retry — это
 * задача более высокого уровня ({@link WorkloadRunner}). Любая ошибка ввода/вывода или
 * UNAVAILABLE-ответ пробрасывается наружу.
 *
 * Объект не потокобезопасен: одно соединение — один пользовательский поток.
 */
public final class KvClient implements Closeable {

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;

    private KvClient(Socket socket, InputStream in, OutputStream out) {
        this.socket = socket;
        this.in = in;
        this.out = out;
    }

    /** Подключиться к {@code 127.0.0.1:port}. */
    public static KvClient connect(int port) throws IOException {
        return connect(InetAddress.getLoopbackAddress().getHostAddress(), port);
    }

    public static KvClient connect(String host, int port) throws IOException {
        Socket s = new Socket(host, port);
        s.setTcpNoDelay(true);
        InputStream in = new BufferedInputStream(s.getInputStream());
        OutputStream out = new BufferedOutputStream(s.getOutputStream());
        return new KvClient(s, in, out);
    }

    /**
     * GET. Возвращает значение, если ключ есть; пусто, если NOT_FOUND.
     * Кидает {@link UnavailableException} при UNAVAILABLE и {@link IOException} при обрыве.
     */
    public OptionalLong get(long key) throws IOException {
        Codec.writeRequest(Request.get(key), out);
        out.flush();
        Response resp = Codec.readResponse(in);
        return switch (resp.status()) {
            case OK -> OptionalLong.of(resp.value());
            case NOT_FOUND -> OptionalLong.empty();
            case UNAVAILABLE -> throw new UnavailableException("сервер вернул UNAVAILABLE на GET");
        };
    }

    /**
     * PUT. Возвращает нормально при OK.
     * Кидает {@link UnavailableException} при UNAVAILABLE и {@link IOException} при обрыве.
     */
    public void put(long key, long value) throws IOException {
        Codec.writeRequest(Request.put(key, value), out);
        out.flush();
        Response resp = Codec.readResponse(in);
        switch (resp.status()) {
            case OK -> { /* ok */ }
            case NOT_FOUND -> throw new IOException("сервер вернул NOT_FOUND на PUT — нарушение протокола");
            case UNAVAILABLE -> throw new UnavailableException("сервер вернул UNAVAILABLE на PUT");
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
