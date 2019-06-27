package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLogId;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class PulsarSagaLogPoolTest {

    PulsarSagaLogPool pool;

    @BeforeMethod
    public void setup() throws PulsarAdminException {
        PulsarSagaLogInitializer initializer = new PulsarSagaLogInitializer();
        Map<String, String> configuration = new LinkedHashMap<>(initializer.configurationOptionsAndDefaults());
        configuration.put("cluster.owner", "distributed-saga-testng");
        configuration.put("cluster.name", "pulsar-saga-log-pool-test");
        configuration.put("cluster.instance-id", "pool-test-01");

        this.pool = initializer.initialize(configuration);

        /*
         * initialize pulsar state
         */

        Tenants tenants = pool.admin.tenants();
        Namespaces namespaces = pool.admin.namespaces();
        Topics topics = pool.admin.topics();
        try {
            List<String> existingTenants = tenants.getTenants();
            if (!existingTenants.contains(pool.tenant)) {
                // create missing tenant
                tenants.createTenant(pool.tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            }
            List<String> existingNamespaces = namespaces.getNamespaces(pool.tenant);
            if (!existingNamespaces.contains(pool.tenant + "/" + pool.namespace)) {
                // create missing namespace
                namespaces.createNamespace(pool.tenant + "/" + pool.namespace);
            }
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }

        // delete all topics in namespace
        List<String> sagaLogTopics = namespaces.getTopics(pool.tenant + "/" + pool.namespace);
        for (String sagaLogTopic : sagaLogTopics) {
            topics.delete(sagaLogTopic);
        }
    }

    @AfterMethod
    public void teardown() {
        pool.shutdown();
    }

    @Test
    public void thatIdForHasCorrectHashcodeEquals() {
        assertEquals(pool.idFor("A", "somelogid"), pool.idFor("A", "somelogid"));
        assertFalse(pool.idFor("A", "somelogid") == pool.idFor("A", "somelogid"));
        assertNotEquals(pool.idFor("A", "somelogid"), pool.idFor("A", "otherlogid"));
        assertNotEquals(pool.idFor("A", "somelogid"), pool.idFor("B", "otherlogid"));
    }

    @Test
    void thatClusterWideLogIdsAreTheSameAsInstanceLocalLogIds() {
        SagaLogId l1 = pool.registerInstanceLocalIdFor("l1");
        pool.connect(l1);
        SagaLogId l2 = pool.registerInstanceLocalIdFor("l2");
        pool.connect(l2);
        SagaLogId x1 = pool.idFor("otherInstance", "x1");
        pool.connect(x1);
        assertEquals(pool.clusterWideLogIds(), Set.of(l1, l2, x1));
    }

    @Test
    void thatConnectExternalProducesANonNullSagaLog() {
        assertNotNull(pool.connectExternal(pool.registerInstanceLocalIdFor("anyId")));
    }
}
