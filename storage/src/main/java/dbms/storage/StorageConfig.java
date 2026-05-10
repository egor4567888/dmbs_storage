package dbms.storage;

/**
 * Параметры тюнинга для {@link Storage}. {@link #defaults()} — продакшн-настройки;
 * тесты обычно фиксируют меньшие пороги, чтобы быстро проходить пути ротации сегмента
 * и чекпоинта.
 */
public record StorageConfig(
        long maxSegmentBytes,
        long checkpointWalBytesThreshold,
        int  commitBatchMax,
        long commitWaitNanos
) {
    public static StorageConfig defaults() {
        return new StorageConfig(
                /* maxSegmentBytes            = */ 16L * 1024 * 1024,
                /* checkpointWalBytesThreshold= */ 64L * 1024 * 1024,
                /* commitBatchMax             = */ 1024,
                /* commitWaitNanos            = */ 200_000L /* 0.2 мс */
        );
    }

    /** Урезанные пороги для тестов — чтобы ротация и чекпоинт срабатывали через несколько ops. */
    public static StorageConfig forTesting(long maxSegmentBytes, long checkpointThreshold) {
        return new StorageConfig(maxSegmentBytes, checkpointThreshold, 256, 200_000L);
    }
}
