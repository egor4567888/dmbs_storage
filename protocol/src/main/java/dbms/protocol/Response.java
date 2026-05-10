package dbms.protocol;

/**
 * Ответ сервера на один запрос. Поле {@code value} имеет смысл только при {@link Status#OK}
 * для GET; в остальных случаях оно игнорируется (заполняется нулём при сериализации).
 */
public record Response(Status status, long value) {

    public static Response ok(long value) {
        return new Response(Status.OK, value);
    }

    public static final Response OK_PUT = new Response(Status.OK, 0L);
    public static final Response NOT_FOUND = new Response(Status.NOT_FOUND, 0L);
    public static final Response UNAVAILABLE = new Response(Status.UNAVAILABLE, 0L);
}
