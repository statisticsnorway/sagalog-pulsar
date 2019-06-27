package no.ssb.sagalog.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PulsarAdminTest {

    @Test
    public void thatBlahBlah() throws PulsarAdminException {
        PulsarSagaLogInitializer initializer = new PulsarSagaLogInitializer();
        Map<String, String> configuration = new LinkedHashMap<>(initializer.configurationOptionsAndDefaults());
        String tenant = "distributed-saga-testng";
        configuration.put("cluster.owner", tenant);
        String namespace = "pulsar-saga-log-pool-test";
        configuration.put("cluster.name", namespace);
        configuration.put("cluster.instance-id", "pool-test-01");

        PulsarSagaLogPool pool = initializer.initialize(configuration);
        PulsarAdmin admin = pool.admin;

        List<String> topics = admin.topics().getList(tenant + "/" + namespace);
        for (String topic : topics) {
            printStats(admin, topic);
        }
    }

    private void printStats(PulsarAdmin admin, String topic) throws PulsarAdminException {
        Topics topics = admin.topics();

        TopicStats stats = topics.getStats(topic);
        List<ConsumerStats> masterConsumers = stats.subscriptions.get("master").consumers;
        if (masterConsumers.size() > 0) {
            ConsumerStats masterConsumer = masterConsumers.get(0);
            System.out.printf("TOPIC: %s, consumerName: %s, address: %s%n", topic, masterConsumer.consumerName, masterConsumer.getAddress());
        } else {
            System.out.printf("TOPIC: %s, No active consumers!%n", topic);
        }
    }
}
