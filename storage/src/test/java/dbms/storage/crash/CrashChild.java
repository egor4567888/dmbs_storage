package dbms.storage.crash;

import dbms.storage.Storage;
import dbms.storage.StorageConfig;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Random;

/**
 * Child-JVM для {@link dbms.storage.CrashTest}. Бесконечно долбит {@code put} и печатает по
 * одной строке {@code OK <key> <value>} на каждый успешный durable-put. Stdout обёрнут с
 * autoFlush=true, чтобы parent видел каждый ACK до того, как решит послать SIGKILL.
 *
 * Args: {@code <stateDir> <maxSegmentBytes> <ckptThresholdBytes> <seed>}.
 *
 * ВАЖНО: этот процесс никогда не устанавливает сигнал-хэндлер — тест в стиле оркестратора шлёт
 * SIGKILL через {@link Process#destroyForcibly()}, и контракт задания требует, чтобы сервер не
 * перехватывал сигналы. Здесь мы соблюдаем то же самое.
 */
public final class CrashChild {

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]);
        long maxSeg = Long.parseLong(args[1]);
        long ckpt = Long.parseLong(args[2]);
        long seed = Long.parseLong(args[3]);

        // autoFlush=true — чтобы каждый println был виден parent-у до SIGKILL.
        PrintStream out = new PrintStream(System.out, true);

        StorageConfig cfg = StorageConfig.forTesting(maxSeg, ckpt);
        Storage s = Storage.open(dir, cfg);
        out.println("READY");

        // Уникальный ключ на каждый put. Зачем: при пересекающихся ключах oracle parent-а
        // отражает только те ACK-и, которые он успел увидеть до SIGKILL. Между моментом
        // получения ACK-а в child и моментом, когда parent прочитал следующую "OK"-строку из
        // pipe, есть гонка. Если child убит после durable-ACK по ключу K, но до того, как эта
        // строка дошла до parent-а, storage (правильно) будет содержать БОЛЕЕ НОВОЕ значение
        // для K, тогда как oracle parent-а — БОЛЕЕ СТАРОЕ. Внешне это выглядит как нарушение
        // durability, хотя на самом деле им не является. С уникальными ключами для верификации
        // имеют значение только те ACK-и, которые parent видел; в storage могут быть лишние
        // ключи (чьи "OK"-строки не успели), и это нормально.
        Random r = new Random(seed);
        long n = 0;
        while (true) {
            long k = n;
            long v = r.nextLong();
            s.put(k, v);
            out.println("OK " + k + " " + v);
            n++;
        }
    }

    private CrashChild() {}
}
