package dbms.protocol;

/**
 * Запрос клиента к серверу. Поле {@code value} имеет смысл только для {@link Op#PUT};
 * для {@link Op#GET} оно игнорируется обеими сторонами.
 */
public record Request(Op op, long key, long value) {

    public static Request get(long key) {
        return new Request(Op.GET, key, 0L);
    }

    public static Request put(long key, long value) {
        return new Request(Op.PUT, key, value);
    }
}
