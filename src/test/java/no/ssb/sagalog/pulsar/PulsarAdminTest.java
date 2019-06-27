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
        configuration.put("cluster.owner", "distributed-saga-testng");
        configuration.put("cluster.name", "pulsar-saga-log-pool-test");
        configuration.put("cluster.instance-id", "pool-test-01");

        PulsarSagaLogPool pool = initializer.initialize(configuration);
        PulsarAdmin admin = pool.admin;

        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:00");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:01");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:02");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:03");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:04");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds01:05");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:00");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:01");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:02");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:03");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:04");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds02:05");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:00");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:01");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:02");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:03");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:04");
        printStats(admin, "persistent://ssb.test/lds-cluster-test/lds03:05");
    }

    private void printStats(PulsarAdmin admin, String topic) throws PulsarAdminException {
        Topics topics = admin.topics();

        //JsonObject jsonObject = topics.getInternalInfo(topic);
        //System.out.printf("%s%n", jsonObject.toString());

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
