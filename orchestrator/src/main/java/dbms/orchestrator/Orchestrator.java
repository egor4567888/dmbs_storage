package dbms.orchestrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Управляет жизненным циклом одного серверного процесса. Цикл такой:
 *   1. fork-ает JVM с указанным main-классом (по умолчанию {@code dbms.server.ServerMain});
 *   2. ждёт строку {@code "LISTENING <port>"} в его stdout — это означает «готов»;
 *   3. спит случайное время в {@code [minLifeMs, maxLifeMs]};
 *   4. шлёт SIGKILL через {@link Process#destroyForcibly()};
 *   5. дожидается смерти процесса, идёт на следующую итерацию.
 *
 *
 * Класс безопасно используется и из CLI, и из тестов:
 *  - {@link #start()} стартует фоновый поток управления;
 *  - {@link #waitUntilReady} блокируется до первого готового сервера;
 *  - {@link #close()} останавливает цикл и убивает текущий процесс.
 */
public final class Orchestrator implements AutoCloseable {

    public static final String DEFAULT_MAIN = "dbms.server.ServerMain";

    private final Path stateDir;
    private final int port;
    private final long minLifeMs;
    private final long maxLifeMs;
    private final String javaExe;
    private final String classpath;
    private final String mainClass;
    private final Random rng;

    private final Thread loopThread;
    private final AtomicReference<Process> currentProcess = new AtomicReference<>();
    private final AtomicReference<Integer> boundPort = new AtomicReference<>();
    private final AtomicInteger restartCount = new AtomicInteger();
    private final AtomicInteger crashKillCount = new AtomicInteger();

    private final Object readyMonitor = new Object();
    private volatile boolean stop;

    public Orchestrator(Path stateDir,
                        int port,
                        long minLifeMs,
                        long maxLifeMs,
                        String javaExe,
                        String classpath,
                        String mainClass,
                        long seed) {
        if (minLifeMs <= 0 || maxLifeMs < minLifeMs) {
            throw new IllegalArgumentException(
                    "ожидается 0 < minLifeMs <= maxLifeMs, получено "
                            + minLifeMs + "/" + maxLifeMs);
        }
        this.stateDir = stateDir;
        this.port = port;
        this.minLifeMs = minLifeMs;
        this.maxLifeMs = maxLifeMs;
        this.javaExe = javaExe;
        this.classpath = classpath;
        this.mainClass = mainClass;
        this.rng = new Random(seed);
        this.loopThread = new Thread(this::supervisionLoop, "kv-orchestrator");
        this.loopThread.setDaemon(true);
    }

    public void start() {
        loopThread.start();
    }

    /**
     * Блокируется, пока не появится готовый (LISTENING) сервер, либо до таймаута.
     * Возвращает порт первого готового сервера. Бросает {@link IllegalStateException}, если
     * оркестратор уже остановлен или таймаут истёк.
     */
    public int waitUntilReady(long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        synchronized (readyMonitor) {
            while (boundPort.get() == null) {
                long remainingMs = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMs <= 0) {
                    throw new IllegalStateException("таймаут ожидания готовности сервера");
                }
                if (stop) {
                    throw new IllegalStateException("оркестратор остановлен");
                }
                readyMonitor.wait(Math.min(remainingMs, 200));
            }
            return boundPort.get();
        }
    }

    public Integer currentPort() {
        return boundPort.get();
    }

    public int restartCount() { return restartCount.get(); }
    public int crashKillCount() { return crashKillCount.get(); }

    @Override
    public void close() {
        stop = true;
        Process p = currentProcess.getAndSet(null);
        if (p != null && p.isAlive()) {
            p.destroyForcibly();
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            loopThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    private void supervisionLoop() {
        while (!stop) {
            Process p;
            try {
                p = spawnServer();
            } catch (IOException e) {
                System.err.println("orchestrator: не удалось запустить сервер: " + e.getMessage());
                sleepInterruptibly(200);
                continue;
            }
            currentProcess.set(p);
            restartCount.incrementAndGet();

            Thread reader = startReadyReader(p);

            long lifeMs = minLifeMs + (long) (rng.nextDouble() * (maxLifeMs - minLifeMs + 1));
            sleepInterruptibly(lifeMs);

            if (stop) {
                p.destroyForcibly();
                try {
                    p.waitFor();
                } catch (InterruptedException ignored) {}
                return;
            }

            p.destroyForcibly();
            crashKillCount.incrementAndGet();
            try {
                p.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            boundPort.set(null);
            try {
                reader.join(500);
            } catch (InterruptedException ignored) {}
        }
    }

    private Process spawnServer() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add(javaExe);
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add(mainClass);
        cmd.add(stateDir.toString());
        cmd.add(Integer.toString(port));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        return pb.start();
    }

    private Thread startReadyReader(Process p) {
        Thread t = new Thread(() -> {
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.startsWith("LISTENING ")) {
                        int parsedPort = Integer.parseInt(line.substring("LISTENING ".length()).trim());
                        synchronized (readyMonitor) {
                            boundPort.set(parsedPort);
                            readyMonitor.notifyAll();
                        }
                    }
                }
            } catch (IOException ignored) {
                // процесс убит, pipe закрыт
            }
        }, "kv-orchestrator-reader");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private void sleepInterruptibly(long ms) {
        long deadline = System.nanoTime() + ms * 1_000_000L;
        while (!stop) {
            long remaining = (deadline - System.nanoTime()) / 1_000_000L;
            if (remaining <= 0) return;
            try {
                Thread.sleep(Math.min(remaining, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
