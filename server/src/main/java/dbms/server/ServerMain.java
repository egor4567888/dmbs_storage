package dbms.server;

import dbms.storage.Storage;
import dbms.storage.StorageConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

/**
 * CLI-точка входа сервера. Аргументы:
 * <pre>
 *   args[0] — путь к директории состояния (stateDir для Storage)
 *   args[1] — TCP-порт; 0 = выбрать свободный (порт будет напечатан в stdout)
 * </pre>
 *
 * Сервер печатает в stdout одну строку {@code "LISTENING <port>\n"} как только готов принимать
 * соединения — оркестратор/тесты ждут эту строку, прежде чем подключаться.
 *
 * Никакого {@code Runtime.addShutdownHook} здесь не регистрируется. SIGTERM/SIGKILL ничем не
 * перехватываются — это требование задания («сервер не должен обрабатывать сигнал»).
 */
public final class ServerMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: ServerMain <stateDir> <port>");
            System.exit(2);
            return;
        }
        Path stateDir = Paths.get(args[0]);
        int port = Integer.parseInt(args[1]);

        Storage storage = Storage.open(stateDir, StorageConfig.defaults());
        KvServer server = KvServer.start(storage, port);


        System.out.println("LISTENING " + server.boundPort());
        System.out.flush();

        new CountDownLatch(1).await();
    }

    private ServerMain() {}
}
