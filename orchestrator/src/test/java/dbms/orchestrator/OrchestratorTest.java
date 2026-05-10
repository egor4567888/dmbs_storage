package dbms.orchestrator;

import dbms.client.KvClient;
import dbms.storage.Storage;
import dbms.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Проверяет, что оркестратор поднимает сервер, к нему можно успешно ходить, и что после
 * SIGKILL и автоматического перезапуска данные сохраняются.
 */
class OrchestratorTest {

    @Test
    void serverIsKilledAndRestartedPreservingData(@TempDir Path stateDir) throws Exception {
        int port = pickFreePort();
        long seed = Long.getLong("orchestrator.test.seed", System.nanoTime());

        try (Orchestrator orch = newOrchestrator(stateDir, port, 300, 600, seed)) {
            orch.start();
            int firstPort = orch.waitUntilReady(10_000);
            assertThat(firstPort).isEqualTo(port);

            try (KvClient c = KvClient.connect(port)) {
                c.put(1L, 100L);
                c.put(2L, 200L);
            }


            long deadline = System.nanoTime() + 5_000_000_000L;
            while (orch.crashKillCount() < 1 && System.nanoTime() < deadline) {
                Thread.sleep(50);
            }
            assertThat(orch.crashKillCount()).isGreaterThanOrEqualTo(1);


            int restartedPort = orch.waitUntilReady(10_000);
            assertThat(restartedPort).isEqualTo(port);

            try (KvClient c = KvClient.connect(port)) {
                assertThat(c.get(1L)).isEqualTo(OptionalLong.of(100L));
                assertThat(c.get(2L)).isEqualTo(OptionalLong.of(200L));
            }
        }


        try (Storage s = Storage.open(stateDir, StorageConfig.defaults())) {
            assertThat(s.get(1L)).isEqualTo(OptionalLong.of(100L));
            assertThat(s.get(2L)).isEqualTo(OptionalLong.of(200L));
        }
    }

    private static Orchestrator newOrchestrator(Path stateDir, int port, long minMs, long maxMs, long seed) {
        String javaExe = Path.of(
                System.getProperty("orchestrator.test.javaHome", System.getProperty("java.home")),
                "bin", "java").toString();
        String classpath = System.getProperty("orchestrator.test.classpath",
                System.getProperty("java.class.path"));
        return new Orchestrator(
                stateDir, port, minMs, maxMs,
                javaExe, classpath, Orchestrator.DEFAULT_MAIN, seed);
    }

    private static int pickFreePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
