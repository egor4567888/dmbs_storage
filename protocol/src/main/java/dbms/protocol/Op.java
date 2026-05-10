package dbms.protocol;

/**
 * Тип операции в запросе клиента. Кодируется одним байтом по проводу.
 */
public enum Op {
    GET((byte) 1),
    PUT((byte) 2);

    private final byte code;

    Op(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    /** Декодирует байт в операцию; null, если код неизвестен. */
    public static Op fromCode(byte code) {
        for (Op op : values()) {
            if (op.code == code) {
                return op;
            }
        }
        return null;
    }
}
