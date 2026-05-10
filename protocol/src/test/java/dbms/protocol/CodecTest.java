package dbms.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CodecTest {

    @Test
    void roundTripGetRequest() throws Exception {
        Request original = Request.get(0xDEADBEEFCAFEBABEL);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Codec.writeRequest(original, out);
        assertThat(out.size()).isEqualTo(Codec.REQUEST_SIZE);

        Request decoded = Codec.readRequest(new ByteArrayInputStream(out.toByteArray()));
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTripPutRequest() throws Exception {
        Request original = Request.put(42L, -1L);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Codec.writeRequest(original, out);

        Request decoded = Codec.readRequest(new ByteArrayInputStream(out.toByteArray()));
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTripOkResponse() throws Exception {
        Response original = Response.ok(123456789L);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Codec.writeResponse(original, out);
        assertThat(out.size()).isEqualTo(Codec.RESPONSE_SIZE);

        Response decoded = Codec.readResponse(new ByteArrayInputStream(out.toByteArray()));
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void roundTripNotFoundAndUnavailable() throws Exception {
        for (Response original : new Response[]{Response.NOT_FOUND, Response.UNAVAILABLE}) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Codec.writeResponse(original, out);
            Response decoded = Codec.readResponse(new ByteArrayInputStream(out.toByteArray()));
            assertThat(decoded).isEqualTo(original);
        }
    }

    @Test
    void readRequestReturnsNullOnImmediateEof() throws Exception {
        Request r = Codec.readRequest(new ByteArrayInputStream(new byte[0]));
        assertThat(r).isNull();
    }

    @Test
    void readRequestThrowsOnTruncatedStream() {
        byte[] partial = new byte[Codec.REQUEST_SIZE / 2];
        assertThatThrownBy(() -> Codec.readRequest(new ByteArrayInputStream(partial)))
                .isInstanceOf(EOFException.class);
    }

    @Test
    void readRequestRejectsUnknownOp() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(99); 
        for (int i = 0; i < 16; i++) out.write(0);
        assertThatThrownBy(() -> Codec.readRequest(new ByteArrayInputStream(out.toByteArray())))
                .hasMessageContaining("неизвестный код операции");
    }
}
