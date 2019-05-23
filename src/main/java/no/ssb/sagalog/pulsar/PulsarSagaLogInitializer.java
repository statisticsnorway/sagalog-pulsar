package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.SagaLogPool;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;

public class PulsarSagaLogInitializer implements SagaLogInitializer {

    public PulsarSagaLogInitializer() {
    }

    @Override
    public SagaLogPool initialize(Map<String, String> configuration) {
        String serviceUrl = configuration.get("pulsar.service.url");
        PulsarClient pulsarClient;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        String tenant = configuration.get("pulsar.sagalog.tenant");
        String namespace = configuration.get("pulsar.sagalog.namespace");
        String applicationName = configuration.get("application.name");
        String applicationInstanceId = configuration.get("application.instance-id");
        return new PulsarSagaLogPool(pulsarClient, tenant, namespace, applicationName, applicationInstanceId);
    }

    @Override
    public Map<String, String> configurationOptionsAndDefaults() {
        return Map.of("pulsar.service.url", "pulsar://localhost:6650",
                "pulsar.admin.service.url", "http://localhost:8080",
                "pulsar.sagalog.tenant", "mycompany",
                "pulsar.sagalog.namespace", "sagalog-test",
                "application.name", "internal-integration-testing",
                "application.instance-id", "01"
        );
    }
}
