package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLogEntryId;
import org.apache.pulsar.client.api.MessageId;

import java.util.Objects;

class PulsarSagaLogEntryId implements SagaLogEntryId {
    final MessageId id;

    PulsarSagaLogEntryId(MessageId id) {
        if (id == null) {
            throw new IllegalArgumentException("id cannot be null");
        }
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarSagaLogEntryId that = (PulsarSagaLogEntryId) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "PulsarSagaLogEntryId{" +
                "id=" + id +
                '}';
    }
}
