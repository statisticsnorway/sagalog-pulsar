import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.pulsar.PulsarSagaLogInitializer;

module no.ssb.sagalog.pulsar {
    requires no.ssb.sagalog;
    requires pulsar.client.admin;
    requires java.sql;

    provides SagaLogInitializer with PulsarSagaLogInitializer;
}