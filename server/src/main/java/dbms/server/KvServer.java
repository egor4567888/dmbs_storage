package dbms.server;

import dbms.protocol.Codec;
import dbms.protocol.Op;
import dbms.protocol.Request;
import dbms.protocol.Response;
import dbms.storage.Storage;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Сетевой слой над {@link Storage}.
 *
 * Архитектура: один accept-thread, поток на соединение. 
 *
 * Контракт по сигналам: сервер не регистрирует никаких хэндлеров. SIGKILL от оркестратора
 * убивает процесс мгновенно, что и требуется заданием.
 *
 * Время жизни одного соединения: клиент пишет фреймы запросов фиксированного размера, сервер
 * на каждый запрос отвечает ровно одним фреймом. Любая ошибка ввода/вывода → лог в stderr и
 * закрытие сокета (клиент сам решит, делать ли backoff и переоткрытие).
 */
public final class KvServer implements AutoCloseable {

    private final Storage storage;
    private final ServerSocket serverSocket;
    private final Thread acceptThread;
    private volatile boolean closed;

    /** Счётчики для диагностики/метрик. Полезны и в тестах, и при ручных прогонах. */
    private final AtomicLong totalGets = new AtomicLong();
    private final AtomicLong totalPuts = new AtomicLong();
    private final AtomicLong totalUnavailable = new AtomicLong();

    private KvServer(Storage storage, ServerSocket serverSocket) {
        this.storage = storage;
        this.serverSocket = serverSocket;
        this.acceptThread = new Thread(this::acceptLoop, "kv-server-accept");
        this.acceptThread.setDaemon(true);
    }

    /**
     * Поднимает TCP-сервер на {@code 127.0.0.1:port}. {@code port=0} → ОС выберет свободный.
     * Реальный порт можно узнать через {@link #boundPort()}.
     */
    public static KvServer start(Storage storage, int port) throws IOException {
        ServerSocket sock = new ServerSocket(port, 64, InetAddress.getLoopbackAddress());
        KvServer s = new KvServer(storage, sock);
        s.acceptThread.start();
        return s;
    }

    public int boundPort() {
        return serverSocket.getLocalPort();
    }

    public long totalGets() { return totalGets.get(); }
    public long totalPuts() { return totalPuts.get(); }
    public long totalUnavailable() { return totalUnavailable.get(); }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        serverSocket.close();
        try {
            acceptThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void acceptLoop() {
        while (!closed) {
            Socket client;
            try {
                client = serverSocket.accept();
            } catch (IOException e) {
                if (closed) return;
                System.err.println("accept failed: " + e.getMessage());
                continue;
            }
            try {
                client.setTcpNoDelay(true);
            } catch (IOException ignored) {
            }
            Thread t = new Thread(() -> serveOne(client), "kv-server-conn-" + client.getPort());
            t.setDaemon(true);
            t.start();
        }
    }

    private void serveOne(Socket socket) {
        try (Socket s = socket;
             InputStream rawIn = s.getInputStream();
             OutputStream rawOut = s.getOutputStream();
             InputStream in = new BufferedInputStream(rawIn);
             OutputStream out = new BufferedOutputStream(rawOut)) {
            while (true) {
                Request req = Codec.readRequest(in);
                if (req == null) {
                    return;
                }
                Response resp = handle(req);
                Codec.writeResponse(resp, out);
                out.flush();
            }
        } catch (IOException e) {
            if (!closed) {
                System.err.println("connection " + socket.getPort() + " closed: " + e.getMessage());
            }
        }
    }

    private Response handle(Request req) {
        try {
            if (req.op() == Op.GET) {
                totalGets.incrementAndGet();
                OptionalLong v = storage.get(req.key());
                return v.isPresent() ? Response.ok(v.getAsLong()) : Response.NOT_FOUND;
            }
            if (req.op() == Op.PUT) {
                storage.put(req.key(), req.value());
                totalPuts.incrementAndGet();
                return Response.OK_PUT;
            }
            totalUnavailable.incrementAndGet();
            return Response.UNAVAILABLE;
        } catch (IOException e) {
            totalUnavailable.incrementAndGet();
            return Response.UNAVAILABLE;
        } catch (IllegalStateException e) {
            totalUnavailable.incrementAndGet();
            return Response.UNAVAILABLE;
        }
    }
}
