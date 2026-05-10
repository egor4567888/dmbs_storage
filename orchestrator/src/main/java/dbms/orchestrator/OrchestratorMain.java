package dbms.orchestrator;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * CLI-точка входа оркестратора. Аргументы:
 * <pre>
 *   args[0] — путь к директории состояния
 *   args[1] — TCP-порт сервера (фиксированный, чтобы клиент знал, куда стучать)
 *   args[2] — минимальное время жизни процесса в мс
 *   args[3] — максимальное время жизни процесса в мс
 *   args[4] — seed (опционально, по умолчанию nanoTime)
 * </pre>
 *
 * Использует тот же java и тот же classpath, на котором запущен сам оркестратор — это
 * самый простой и предсказуемый способ собрать дочернюю команду.
 */
public final class OrchestratorMain {

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5) {
            System.err.println("usage: OrchestratorMain <stateDir> <port> <minLifeMs> <maxLifeMs> [seed]");
            System.exit(2);
            return;
        }
        Path stateDir = Paths.get(args[0]);
        int port = Integer.parseInt(args[1]);
        long minLifeMs = Long.parseLong(args[2]);
        long maxLifeMs = Long.parseLong(args[3]);
        long seed = args.length > 4 ? Long.parseLong(args[4]) : System.nanoTime();

        String javaExe = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        Orchestrator orch = new Orchestrator(
                stateDir, port, minLifeMs, maxLifeMs,
                javaExe, classpath, Orchestrator.DEFAULT_MAIN, seed);
        orch.start();
        Thread.currentThread().join();
    }

    private OrchestratorMain() {}
}
