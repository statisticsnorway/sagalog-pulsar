import no.ssb.saga.execution.sagalog.SagaLogInitializer;
import no.ssb.sagalog.pulsar.PulsarSagaLogInitializer;

module no.ssb.sagalog.pulsar {
    requires pulsar.client.api;
    requires pulsar.client.original;
    requires pulsar.client.admin.original;
    requires java.sql;
    requires org.slf4j;

    provides SagaLogInitializer with PulsarSagaLogInitializer;
}