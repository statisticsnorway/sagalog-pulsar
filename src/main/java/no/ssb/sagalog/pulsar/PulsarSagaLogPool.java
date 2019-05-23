package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogPool;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

class PulsarSagaLogPool implements SagaLogPool {

    private final PulsarClient client;
    private final String tenant;
    private final String namespace;
    private final String application;
    private final String applicationInstanceId;

    private final Map<String, PulsarSagaLog> sagaLogByLogId = new ConcurrentHashMap<>();
    private final Set<String> occupied = new ConcurrentSkipListSet<>();

    PulsarSagaLogPool(PulsarClient client, String tenant, String namespace, String application, String applicationInstanceId) {
        this.client = client;
        this.tenant = tenant;
        this.namespace = namespace;
        this.application = application;
        this.applicationInstanceId = applicationInstanceId;
    }

    @Override
    public SagaLog connect(String logId) {
        if (!occupied.add(logId)) {
            throw new RuntimeException(String.format("saga-log with logId \"%s\" already connected."));
        }
        PulsarSagaLog sagaLog = sagaLogByLogId.computeIfAbsent(logId, lid -> {
            String topic = "persistent://" + tenant + "/" + namespace + "/" + application + "-" + applicationInstanceId + "-" + lid;
            return new PulsarSagaLog(client, topic, application, applicationInstanceId);
        });
        return sagaLog;
    }

    @Override
    public void release(String logId) {
        occupied.remove(logId);
    }

    @Override
    public void shutdown() {
        try {
            for (PulsarSagaLog sagaLog : sagaLogByLogId.values()) {
                sagaLog.close();
            }
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                client.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
