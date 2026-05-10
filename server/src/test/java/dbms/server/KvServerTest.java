package dbms.server;

import dbms.protocol.Codec;
import dbms.protocol.Request;
import dbms.protocol.Response;
import dbms.protocol.Status;
import dbms.storage.Storage;
import dbms.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Поднимаем сервер в текущей JVM, ходим к нему через настоящий TCP — проверяем end-to-end
 * протокол и счётчики.
 */
class KvServerTest {

    @Test
    void putThenGetReturnsValue(@TempDir Path dir) throws Exception {
        try (Storage storage = Storage.open(dir, StorageConfig.defaults());
             KvServer server = KvServer.start(storage, 0);
             Socket sock = new Socket(InetAddress.getLoopbackAddress(), server.boundPort())) {

            sock.setTcpNoDelay(true);
            try (InputStream in = new BufferedInputStream(sock.getInputStream());
                 OutputStream out = new BufferedOutputStream(sock.getOutputStream())) {

                Codec.writeRequest(Request.put(42L, 100L), out);
                out.flush();
                Response putResp = Codec.readResponse(in);
                assertThat(putResp.status()).isEqualTo(Status.OK);

                Codec.writeRequest(Request.get(42L), out);
                out.flush();
                Response getResp = Codec.readResponse(in);
                assertThat(getResp).isEqualTo(Response.ok(100L));

                Codec.writeRequest(Request.get(999L), out);
                out.flush();
                Response missResp = Codec.readResponse(in);
                assertThat(missResp).isEqualTo(Response.NOT_FOUND);
            }

            assertThat(server.totalPuts()).isEqualTo(1);
            assertThat(server.totalGets()).isEqualTo(2);
            assertThat(server.totalUnavailable()).isZero();
        }
    }

    @Test
    void multipleConnectionsServedConcurrently(@TempDir Path dir) throws Exception {
        try (Storage storage = Storage.open(dir, StorageConfig.defaults());
             KvServer server = KvServer.start(storage, 0)) {

            int connections = 4;
            int opsPerConn = 50;
            Thread[] threads = new Thread[connections];
            for (int i = 0; i < connections; i++) {
                final int connId = i;
                threads[i] = new Thread(() -> runWorkload(server.boundPort(), connId, opsPerConn));
                threads[i].start();
            }
            for (Thread t : threads) {
                t.join(10_000);
            }

            assertThat(server.totalPuts()).isEqualTo((long) connections * opsPerConn);
        }
    }

    private void runWorkload(int port, int connId, int ops) {
        try (Socket sock = new Socket(InetAddress.getLoopbackAddress(), port)) {
            sock.setTcpNoDelay(true);
            try (InputStream in = new BufferedInputStream(sock.getInputStream());
                 OutputStream out = new BufferedOutputStream(sock.getOutputStream())) {
                for (int i = 0; i < ops; i++) {
                    long key = ((long) connId << 32) | i;
                    Codec.writeRequest(Request.put(key, key + 1), out);
                    out.flush();
                    Response resp = Codec.readResponse(in);
                    if (resp.status() != Status.OK) {
                        throw new AssertionError("ожидали OK, получили " + resp);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
