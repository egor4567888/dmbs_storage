package dbms.integration;

import dbms.client.LocalReplica;
import dbms.client.WorkloadRunner;
import dbms.orchestrator.Orchestrator;
import dbms.storage.Storage;
import dbms.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Интеграционный сценарий: оркестратор + сервер (в дочерней JVM) + N клиентов в текущей JVM.
 * Оркестратор регулярно убивает сервер, клиенты долбят запросами и сами проверяют ответы.
 *
 * Что доказывает этот тест:
 *
 *  1. Под крэш-тестингом ни один клиент не зафиксировал
 *     {@code verificationFailures > 0} → значит, для каждого «уверенного» ключа сервер всегда
 *     отвечает тем, что мы ему писали.
 *  2. После остановки оркестратора и финальной {@link Storage#open} напрямую — каждый
 *     {@code committed} от каждого клиента виден в storage с тем же значением. Это и есть
 *     durability в условиях аварийных перезапусков.
 *  3. Параллельно набегает throughput, который мы печатаем в лог.
 */
class ChaosTest {

    @Test
    void clientsAndOrchestratorAgreeAfterRandomKills(@TempDir Path stateDir) throws Exception {
        long topSeed = Long.getLong("integration.chaos.seed", System.nanoTime());
        long durationMs = Long.getLong("integration.chaos.durationMs", 15_000L);
        int numClients = Integer.getInteger("integration.chaos.clients", 3);
        long minLifeMs = Long.getLong("integration.chaos.minLifeMs", 700L);
        long maxLifeMs = Long.getLong("integration.chaos.maxLifeMs", 1_800L);
        long keysPerClient = Long.getLong("integration.chaos.keysPerClient", 4096L);

        int port = pickFreePort();

        Orchestrator orch = newOrchestrator(stateDir, port, minLifeMs, maxLifeMs, topSeed);
        orch.start();
        try {
            orch.waitUntilReady(15_000);

            // Поднимаем клиентов в этой JVM. Каждый получает свой непересекающийся диапазон
            // ключей — это снимает затирание чужих записей и делает верификацию реплики
            // строгой («сервер вернёт ровно то, что я туда положил»).
            List<WorkloadRunner> runners = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < numClients; i++) {
                long keyStart = (long) i * keysPerClient;
                WorkloadRunner r = new WorkloadRunner(
                        port, keyStart, keysPerClient, topSeed ^ (1L + i));
                Thread t = new Thread(r, "chaos-client-" + i);
                t.setDaemon(true);
                runners.add(r);
                threads.add(t);
                t.start();
            }

            // Хаос идёт.
            Thread.sleep(durationMs);

            // Останавливаем клиентов (мягко — они сами выйдут после текущей итерации).
            for (WorkloadRunner r : runners) r.stop();
            for (Thread t : threads) t.join(TimeUnit.SECONDS.toMillis(15));

            // Останавливаем оркестратор (он ещё и убьёт текущий серверный процесс).
            // Это важно сделать ДО прямого Storage.open: иначе оркестратор перезапустит сервер
            // и тот возьмёт state-dir эксклюзивно (через WAL).
            orch.close();

            // Сводный лог по клиентам.
            long totalOps = 0;
            long totalFailures = 0;
            for (int i = 0; i < runners.size(); i++) {
                WorkloadRunner r = runners.get(i);
                LocalReplica rep = r.replica();
                System.out.printf(
                        "client #%d: ops=%d reconnects=%d committed=%d unknown=%d failures=%d%n",
                        i, r.opsCompleted(), r.reconnects(),
                        rep.committedCount(), rep.unknownCount(), rep.verificationFailures());
                if (rep.verificationFailures() > 0) {
                    System.out.println("    first failure: " + rep.firstFailureDetail());
                }
                if (r.terminalError() != null) {
                    r.terminalError().printStackTrace();
                }
                totalOps += r.opsCompleted();
                totalFailures += rep.verificationFailures();
            }
            double seconds = durationMs / 1000.0;
            System.out.printf("aggregate: ops=%d throughput=%.0f ops/s%n",
                    totalOps, totalOps / seconds);
            System.out.printf("orchestrator: restarts=%d crashKills=%d%n",
                    orch.restartCount(), orch.crashKillCount());

            // Главные инварианты.
            assertThat(totalFailures)
                    .as("сервер должен отвечать согласовано с локальной репликой каждого клиента")
                    .isZero();
            assertThat(orch.crashKillCount())
                    .as("оркестратор обязан был успеть убить сервер хотя бы раз")
                    .isGreaterThanOrEqualTo(1);
            assertThat(totalOps)
                    .as("клиенты должны были выполнить хоть какие-то операции")
                    .isGreaterThan(0);

            // Финальная durability: открываем Storage напрямую и сверяем каждый committed.
            // Диапазоны ключей не пересекаются, поэтому для committed-ключа клиент — единственный
            // писатель. Unknown-ключи пропускаем: там последний PUT мог стать durable до того,
            // как обрыв соединения помешал получить ACK, и тогда сервер содержит новое значение,
            // которое клиент не успел зафиксировать. Ничьей ошибки в этом нет, проверять
            // нечего.
            try (Storage storage = Storage.open(stateDir, StorageConfig.defaults())) {
                int verifiedKeys = 0;
                int skippedUnknown = 0;
                for (int i = 0; i < runners.size(); i++) {
                    Map<Long, Long> committed = runners.get(i).replica().snapshotCommitted();
                    Set<Long> unknown = runners.get(i).replica().snapshotUnknown();
                    for (Map.Entry<Long, Long> e : committed.entrySet()) {
                        if (unknown.contains(e.getKey())) {
                            skippedUnknown++;
                            continue;
                        }
                        OptionalLong got = storage.get(e.getKey());
                        assertThat(got)
                                .as("client #%d key=%d expected=%d", i, e.getKey(), e.getValue())
                                .hasValue(e.getValue());
                        verifiedKeys++;
                    }
                }
                System.out.printf("final durability: verified=%d skipped(unknown)=%d%n",
                        verifiedKeys, skippedUnknown);
            }
        } finally {
            // На случай, если предыдущий orch.close() не отработал из-за исключения.
            orch.close();
        }
    }

    private static Orchestrator newOrchestrator(Path stateDir, int port,
                                                long minMs, long maxMs, long seed) {
        String javaExe = Path.of(
                System.getProperty("integration.test.javaHome", System.getProperty("java.home")),
                "bin", "java").toString();
        String classpath = System.getProperty("integration.test.classpath",
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
