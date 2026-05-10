package dbms.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Состояние KV в оперативной памяти. Чтения — без блокировок; записи проходят через
 * commit-конвейер и фактически выполняются одним потоком. {@link #snapshotCopy()} используется
 * на пути чекпоинта; итерация по {@link ConcurrentHashMap} слабо консистентна — это безопасно,
 * потому что во время чекпоинта параллельных записей в карту нет (см. ARCHITECTURE.md §8.3).
 */
final class KvState {

    private final ConcurrentHashMap<Long, Long> map;

    KvState() {
        this.map = new ConcurrentHashMap<>();
    }

    KvState(Map<Long, Long> initial) {
        this.map = new ConcurrentHashMap<>(initial);
    }

    void put(long key, long value) {
        map.put(key, value);
    }

    OptionalLong get(long key) {
        Long v = map.get(key);
        return v == null ? OptionalLong.empty() : OptionalLong.of(v);
    }

    /** Возвращает point-in-time копию карты для записи в снапшот. */
    Map<Long, Long> snapshotCopy() {
        return new HashMap<>(map);
    }

    int size() {
        return map.size();
    }
}
