package dbms.client;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Юнит-тесты на машину состояний реплики. Сетевая часть здесь не задействована — 
 * проверяем, что переходы по событиям put/get согласованы с моделью.
 */
class LocalReplicaTest {

    @Test
    void putOkUpdatesCommitted() {
        LocalReplica r = new LocalReplica();
        r.recordPutOk(1L, 100L);
        assertThat(r.verifyGet(1L, OptionalLong.of(100L))).isTrue();
        assertThat(r.verificationFailures()).isZero();
    }

    @Test
    void verifyGetCatchesMismatch() {
        LocalReplica r = new LocalReplica();
        r.recordPutOk(1L, 100L);
        boolean ok = r.verifyGet(1L, OptionalLong.of(999L));
        assertThat(ok).isFalse();
        assertThat(r.verificationFailures()).isEqualTo(1);
        assertThat(r.firstFailureDetail()).contains("expected=100", "got=999");
    }

    @Test
    void verifyGetAcceptsAbsentForUnknownKey() {
        LocalReplica r = new LocalReplica();
        assertThat(r.verifyGet(42L, OptionalLong.empty())).isTrue();
        assertThat(r.verifyGet(42L, OptionalLong.of(0L))).isFalse();
    }

    @Test
    void unknownKeyAcceptsEitherAndConverges() {
        LocalReplica r = new LocalReplica();
        r.recordPutUncertain(7L);
        assertThat(r.unknownCount()).isEqualTo(1);
        assertThat(r.verifyGet(7L, OptionalLong.of(55L))).isTrue();
        assertThat(r.unknownCount()).isZero();
        assertThat(r.verificationFailures()).isZero();

        assertThat(r.verifyGet(7L, OptionalLong.of(55L))).isTrue();
        assertThat(r.verifyGet(7L, OptionalLong.of(56L))).isFalse();
    }

    @Test
    void unknownKeyAcceptsAbsentResponse() {
        LocalReplica r = new LocalReplica();
        r.recordPutOk(7L, 100L);

        r.recordPutUncertain(7L);

        assertThat(r.verifyGet(7L, OptionalLong.empty())).isTrue();
        assertThat(r.verifyGet(7L, OptionalLong.empty())).isTrue();
    }

    @Test
    void putRejectedDoesNotChangeReplica() {
        LocalReplica r = new LocalReplica();
        r.recordPutOk(1L, 100L);
        r.recordPutRejected(1L); 

        assertThat(r.verifyGet(1L, OptionalLong.of(100L))).isTrue();
        assertThat(r.verificationFailures()).isZero();
    }
}
