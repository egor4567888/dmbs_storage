package dbms.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {

    @Test
    void roundTrip(@TempDir Path tmp) throws IOException {
        Map<Long, Long> in = new HashMap<>();
        for (long i = 0; i < 1000; i++) in.put(i, i * 7);
        Snapshot.write(tmp, 12345L, in);

        Snapshot.Loaded out = Snapshot.readIfPresent(tmp);
        assertThat(out).isNotNull();
        assertThat(out.appliedLsn()).isEqualTo(12345L);
        assertThat(out.state()).isEqualTo(in);
    }

    @Test
    void emptyMap(@TempDir Path tmp) throws IOException {
        Snapshot.write(tmp, 0L, Map.of());
        Snapshot.Loaded out = Snapshot.readIfPresent(tmp);
        assertThat(out).isNotNull();
        assertThat(out.appliedLsn()).isZero();
        assertThat(out.state()).isEmpty();
    }

    @Test
    void missingFileReturnsNull(@TempDir Path tmp) throws IOException {
        assertThat(Snapshot.readIfPresent(tmp)).isNull();
    }

    @Test
    void corruptPayloadReturnsNull(@TempDir Path tmp) throws IOException {
        Map<Long, Long> in = new HashMap<>();
        for (long i = 0; i < 5; i++) in.put(i, i);
        Snapshot.write(tmp, 1L, in);
        Path file = tmp.resolve(Snapshot.FILE_NAME);

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            // переворачиваем бит в payload-секции (сразу за заголовком)
            long pos = 32; // заголовок 28 байт; первый ключ начинается с offset=28
            ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, pos); one.flip();
            byte b = one.get();
            ByteBuffer flipped = ByteBuffer.wrap(new byte[]{(byte) (b ^ 0x55)});
            ch.write(flipped, pos);
        }
        assertThat(Snapshot.readIfPresent(tmp)).isNull();
    }

    @Test
    void leftoverTmpDoesNotConfuseRead(@TempDir Path tmp) throws IOException {
        Snapshot.write(tmp, 1L, Map.of(1L, 1L));
        Files.writeString(tmp.resolve(Snapshot.TMP_NAME), "garbage");
        Snapshot.Loaded out = Snapshot.readIfPresent(tmp);
        assertThat(out).isNotNull();
        assertThat(out.appliedLsn()).isEqualTo(1L);

        Snapshot.cleanupTmp(tmp);
        assertThat(Files.exists(tmp.resolve(Snapshot.TMP_NAME))).isFalse();
    }
}
