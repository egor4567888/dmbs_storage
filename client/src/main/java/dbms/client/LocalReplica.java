package dbms.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Локальная реплика состояния хранилища с точки зрения одного клиента.
 *
 * Жизненный цикл записи:
 *
 *  - PUT(k, v) → OK от сервера: записываем {@code committed[k] = v}, убираем k из unknown.
 *    Это «золотой» put — мы знаем, что значение durable.
 *  - PUT(k, v) → UNAVAILABLE: сервер ответил, но не применил. Состояние ключа на сервере не
 *    изменилось. Реплика тоже не меняется. Из unknown НЕ убираем (если там было).
 *  - PUT(k, v) → IOException (обрыв соединения, etc.): неоднозначность — запрос мог
 *    применится и стать durable до того, как сервер успел отправить ответ. Кладём k в
 *    {@code unknown}. Получим определённость на следующем GET.
 *
 *  - GET(k) → OK(v):
 *      • k в unknown → сервер всё знает; обновляем committed[k] = v и убираем из unknown.
 *      • k не в unknown → проверяем, что v совпадает с committed[k]. Несовпадение — ошибка.
 *  - GET(k) → NOT_FOUND:
 *      • k в unknown → значит, последний put не успел стать durable; убираем committed[k]
 *        (если был) и из unknown. Это наш способ синхронизироваться с сервером.
 *      • k не в unknown → проверяем, что committed тоже не содержит k.
 *
 * Класс предполагает однопоточное использование (один WorkloadRunner — одна LocalReplica).
 */
public final class LocalReplica {

    private final Map<Long, Long> committed = new HashMap<>();
    private final Set<Long> unknown = new HashSet<>();

    /** Зафиксированы ли ошибки верификации с момента старта. */
    private long verificationFailures;
    private String firstFailureDetail;

    public void recordPutOk(long key, long value) {
        committed.put(key, value);
        unknown.remove(key);
    }

    /**
     * Сервер ответил UNAVAILABLE на PUT — значит, операция точно не применилась. Состояние
     * локальной реплики не меняется.
     */
    public void recordPutRejected(long key) {
        // ничего не делаем; ключ как был, так и остался
    }

    /**
     * PUT упал по таймауту/обрыву — мы не знаем, применился он или нет. Помечаем ключ как
     * неопределённый. Следующий GET снимет неопределённость.
     */
    public void recordPutUncertain(long key) {
        unknown.add(key);
    }

    /**
     * Сверить ответ GET с локальной репликой. Возвращает true, если всё согласовано. При false —
     * увеличивает счётчик verificationFailures и сохраняет детали первой ошибки для диагностики.
     */
    public boolean verifyGet(long key, OptionalLong serverValue) {
        if (unknown.contains(key)) {
            if (serverValue.isPresent()) {
                committed.put(key, serverValue.getAsLong());
            } else {
                committed.remove(key);
            }
            unknown.remove(key);
            return true;
        }
        Long expected = committed.get(key);
        boolean ok;
        if (expected == null) {
            ok = serverValue.isEmpty();
        } else {
            ok = serverValue.isPresent() && serverValue.getAsLong() == expected;
        }
        if (!ok) {
            verificationFailures++;
            if (firstFailureDetail == null) {
                firstFailureDetail = "key=" + key
                        + " expected=" + (expected == null ? "<absent>" : expected)
                        + " got=" + (serverValue.isEmpty() ? "<absent>" : serverValue.getAsLong());
            }
        }
        return ok;
    }

    public long verificationFailures() {
        return verificationFailures;
    }

    public String firstFailureDetail() {
        return firstFailureDetail;
    }

    public int committedCount() {
        return committed.size();
    }

    public int unknownCount() {
        return unknown.size();
    }

    /** Снимок зафиксированных пар, для финальной сверки в тестах. */
    public Map<Long, Long> snapshotCommitted() {
        return new HashMap<>(committed);
    }

    /**
     * Снимок ключей в неопределённом состоянии (последний PUT свалился по обрыву и не был
     * разрешён последующим GET). Используется в финальной сверке тестов: для таких ключей
     * сервер вправе содержать как старое committed-значение, так и новое — мы это не проверяем.
     */
    public Set<Long> snapshotUnknown() {
        return new HashSet<>(unknown);
    }
}
