package dbms.protocol;

/**
 * Код результата операции в ответе сервера.
 *
 *  - {@link #OK} — операция выполнена. Для GET в поле value лежит значение, для PUT поле value
 *    игнорируется (заполняется нулём).
 *  - {@link #NOT_FOUND} — только для GET: ключа нет в хранилище.
 *  - {@link #UNAVAILABLE} — сервер не смог выполнить запрос (например, Storage в fatal-состоянии);
 *    клиенту следует сделать backoff и переоткрыть соединение.
 */
public enum Status {
    OK((byte) 0),
    NOT_FOUND((byte) 1),
    UNAVAILABLE((byte) 2);

    private final byte code;

    Status(byte code) {
        this.code = code;
    }

    public byte code() {
        return code;
    }

    public static Status fromCode(byte code) {
        for (Status s : values()) {
            if (s.code == code) {
                return s;
            }
        }
        return null;
    }
}
