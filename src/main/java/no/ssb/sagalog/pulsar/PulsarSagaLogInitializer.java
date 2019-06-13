package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLogInitializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

import java.util.Map;

public class PulsarSagaLogInitializer implements SagaLogInitializer {

    public PulsarSagaLogInitializer() {
    }

    @Override
    public PulsarSagaLogPool initialize(Map<String, String> configuration) {
        String serviceUrl = configuration.get("pulsar.service.url");
        PulsarClient pulsarClient;
        try {
            pulsarClient = PulsarClient.builder().serviceUrl(serviceUrl).build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
        String tenant = configuration.get("cluster.owner");
        String namespace = configuration.get("cluster.name");
        String instanceId = configuration.get("cluster.instance-id");

        String adminServiceUrl = configuration.get("pulsar.admin.service.url");
        ClientConfigurationData config = new ClientConfigurationData();
        config.setAuthentication(new AuthenticationDisabled());
        config.setServiceUrl(adminServiceUrl);
        PulsarAdmin pulsarAdmin;
        try {
            pulsarAdmin = new PulsarAdmin(adminServiceUrl, config);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        return new PulsarSagaLogPool(pulsarAdmin, pulsarClient, tenant, namespace, instanceId);
    }

    @Override
    public Map<String, String> configurationOptionsAndDefaults() {
        return Map.of("pulsar.service.url", "pulsar://localhost:6650",
                "pulsar.admin.service.url", "http://localhost:8080",
                "cluster.owner", "mycompany",
                "cluster.name", "internal-sagalog-integration-testing",
                "cluster.instance-id", "01"
        );
    }
}
