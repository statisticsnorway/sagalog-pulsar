package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogEntryBuilder;
import no.ssb.sagalog.SagaLogId;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

@Test(groups = "integration")
public class PulsarSagaLogTest {

    private static final SagaLogId SAGA_LOG_ID = new SagaLogId("testng-main-thread");

    private PulsarSagaLog sagaLog;

    @BeforeClass
    public void initializePulsarTenantAndNamespace() throws PulsarClientException, PulsarAdminException {
        String adminServiceUrl = "http://localhost:8080";

        ClientConfigurationData config = new ClientConfigurationData();
        config.setAuthentication(new AuthenticationDisabled());
        config.setServiceUrl(adminServiceUrl);

        try (PulsarAdmin admin = new PulsarAdmin(adminServiceUrl, config)) {

            String tenant = "mycompany";
            String namespace = "internal-sagalog-integration-testing";
            deleteAllTopicsAndNamespaces(admin, tenant);

            if (!admin.tenants().getTenants().contains(tenant)) {
                admin.tenants().createTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            } else {
                admin.tenants().updateTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            }
            List<String> namespaces = admin.namespaces().getNamespaces(tenant);
            if (!namespaces.contains(tenant + "/" + namespace)) {
                admin.namespaces().createNamespace(tenant + "/" + namespace, new Policies());
            }
        }
    }

    @BeforeMethod
    private void createAndCleanPulsarSagaLog() throws PulsarClientException {
        sagaLog = new PulsarSagaLog(
                SAGA_LOG_ID,
                PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build(),
                "internal-sagalog-integration-testing",
                "01");
        sagaLog.truncate().join();
    }

    @AfterMethod
    private void closePulsarSagaLog() throws PulsarClientException {
        sagaLog.close();
    }

    private void deleteAllTopicsAndNamespaces(PulsarAdmin admin, String tenant) {
        try {
            Namespaces namespaces = admin.namespaces();
            Topics topics = admin.topics();
            // delete all topics and namespaces for configured tenant
            List<String> existingNamespaces = namespaces.getNamespaces(tenant);
            for (String namespace : existingNamespaces) {
                for (String topic : topics.getList(namespace)) {
                    for (String subscription : topics.getSubscriptions(topic)) {
                        System.out.format("Deleting subscription %s from topic %s...%n", subscription, topic);
                        topics.deleteSubscription(topic, subscription);
                        System.out.format("Deleted subscription %s from topic %s%n", subscription, topic);
                    }
                    topics.delete(topic);
                }
                namespaces.deleteNamespace(namespace);
            }
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void thatWriteAndReadEntriesWorks() {
        Deque<SagaLogEntry> expectedEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());

        assertEquals(sagaLog.readIncompleteSagas().collect(Collectors.toList()), expectedEntries);
    }

    @Test
    public void thatTruncateWithReadIncompleteWorks() {
        Deque<SagaLogEntry> initialEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());
        sagaLog.truncate(initialEntries.getLast().getId());

        Deque<SagaLogEntry> expectedEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());

        List<SagaLogEntry> actualEntries = sagaLog.readIncompleteSagas().collect(Collectors.toList());
        assertEquals(actualEntries, expectedEntries);
    }

    @Test
    public void thatNoTruncateWithReadIncompleteWorks() {
        Deque<SagaLogEntry> firstEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());
        Deque<SagaLogEntry> secondEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());
        Deque<SagaLogEntry> expectedEntries = new LinkedList<>();
        expectedEntries.addAll(firstEntries);
        expectedEntries.addAll(secondEntries);

        List<SagaLogEntry> actualEntries = sagaLog.readIncompleteSagas().collect(Collectors.toList());
        assertEquals(actualEntries, expectedEntries);
    }

    @Test
    public void thatSnapshotOfSagaLogEntriesByNodeIdWorks() {
        Deque<SagaLogEntry> firstEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());
        Deque<SagaLogEntry> secondEntries = writeSuccessfulVanillaSagaExecutionEntries(sagaLog, UUID.randomUUID().toString());
        Deque<SagaLogEntry> expectedEntries = new LinkedList<>();
        expectedEntries.addAll(firstEntries);
        expectedEntries.addAll(secondEntries);

        Map<String, List<SagaLogEntry>> snapshotFirst = sagaLog.getSnapshotOfSagaLogEntriesByNodeId(firstEntries.getFirst().getExecutionId());
        Set<SagaLogEntry> firstFlattenedSnapshot = new LinkedHashSet<>();
        for (List<SagaLogEntry> collection : snapshotFirst.values()) {
            firstFlattenedSnapshot.addAll(collection);
        }
        Map<String, List<SagaLogEntry>> snapshotSecond = sagaLog.getSnapshotOfSagaLogEntriesByNodeId(secondEntries.getFirst().getExecutionId());
        Set<SagaLogEntry> secondFlattenedSnapshot = new LinkedHashSet<>();
        for (List<SagaLogEntry> collection : snapshotSecond.values()) {
            secondFlattenedSnapshot.addAll(collection);
        }

        assertEquals(firstFlattenedSnapshot, Set.copyOf(firstEntries));
        assertEquals(secondFlattenedSnapshot, Set.copyOf(secondEntries));
    }

    private Deque<SagaLogEntry> writeSuccessfulVanillaSagaExecutionEntries(SagaLog sagaLog, String executionId) {
        Deque<SagaLogEntryBuilder> entryBuilders = new LinkedList<>();
        entryBuilders.add(sagaLog.builder().startSaga(executionId, "Vanilla-Saga", "{}"));
        entryBuilders.add(sagaLog.builder().startAction(executionId, "action1"));
        entryBuilders.add(sagaLog.builder().startAction(executionId, "action2"));
        entryBuilders.add(sagaLog.builder().endAction(executionId, "action1", "{}"));
        entryBuilders.add(sagaLog.builder().endAction(executionId, "action2", "{}"));
        entryBuilders.add(sagaLog.builder().endSaga(executionId));

        Deque<SagaLogEntry> entries = new LinkedList<>();
        for (SagaLogEntryBuilder builder : entryBuilders) {
            CompletableFuture<SagaLogEntry> entryFuture = sagaLog.write(builder);
            entries.add(entryFuture.join());
        }
        return entries;
    }
}
