package dbms.client;

import dbms.server.KvServer;
import dbms.storage.Storage;
import dbms.storage.StorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


class KvClientTest {

    @Test
    void getMissReturnsEmpty(@TempDir Path dir) throws Exception {
        try (Storage storage = Storage.open(dir, StorageConfig.defaults());
             KvServer server = KvServer.start(storage, 0);
             KvClient client = KvClient.connect(server.boundPort())) {
            assertThat(client.get(123L)).isEqualTo(OptionalLong.empty());
        }
    }

    @Test
    void putThenGet(@TempDir Path dir) throws Exception {
        try (Storage storage = Storage.open(dir, StorageConfig.defaults());
             KvServer server = KvServer.start(storage, 0);
             KvClient client = KvClient.connect(server.boundPort())) {
            client.put(7L, 42L);
            assertThat(client.get(7L)).isEqualTo(OptionalLong.of(42L));
        }
    }

    @Test
    void disconnectDuringPutThrowsIOException(@TempDir Path dir) throws Exception {
        try (Storage storage = Storage.open(dir, StorageConfig.defaults());
             KvServer server = KvServer.start(storage, 0)) {
            KvClient client = KvClient.connect(server.boundPort());
            client.close();
            assertThatThrownBy(() -> client.put(1L, 2L)).isInstanceOf(IOException.class);
        }
    }
}
