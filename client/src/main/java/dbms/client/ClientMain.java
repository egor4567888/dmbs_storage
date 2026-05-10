package dbms.client;

import java.util.concurrent.TimeUnit;

/**
 * CLI-точка входа клиента. Аргументы:
 * <pre>
 *   args[0] — порт сервера
 *   args[1] — длительность работы в секундах (0 = вечно, до Ctrl-C)
 *   args[2] — размер пространства ключей (опционально, по умолчанию 4096)
 *   args[3] — seed для ГСЧ (опционально, по умолчанию nanoTime)
 * </pre>
 *
 * Печатает в stdout итоговую статистику: количество операций, реконнектов, нарушений
 * верификации. Завершается с кодом 1 при наличии нарушений верификации.
 */
public final class ClientMain {

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2 || args.length > 4) {
            System.err.println("usage: ClientMain <port> <durationSec> [keySpaceSize] [seed]");
            System.exit(2);
            return;
        }
        int port = Integer.parseInt(args[0]);
        long durationSec = Long.parseLong(args[1]);
        long keySpace = args.length > 2 ? Long.parseLong(args[2]) : 4096L;
        long seed = args.length > 3 ? Long.parseLong(args[3]) : System.nanoTime();

        WorkloadRunner runner = new WorkloadRunner(port, keySpace, seed);
        Thread t = new Thread(runner, "kv-client-workload");
        t.setDaemon(false);
        long startNs = System.nanoTime();
        t.start();

        if (durationSec > 0) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSec));
            runner.stop();
            t.join();
        } else {
            t.join();
        }
        long elapsedNs = System.nanoTime() - startNs;
        double seconds = elapsedNs / 1e9;
        long ops = runner.opsCompleted();

        System.out.printf("ops=%d reconnects=%d duration=%.2fs throughput=%.0f ops/s%n",
                ops, runner.reconnects(), seconds, ops / Math.max(1e-9, seconds));
        System.out.printf("committed=%d unknown=%d verificationFailures=%d%n",
                runner.replica().committedCount(),
                runner.replica().unknownCount(),
                runner.replica().verificationFailures());
        if (runner.replica().verificationFailures() > 0) {
            System.out.println("first failure: " + runner.replica().firstFailureDetail());
            System.exit(1);
        }
        if (runner.terminalError() != null) {
            runner.terminalError().printStackTrace();
            System.exit(2);
        }
    }

    private ClientMain() {}
}
