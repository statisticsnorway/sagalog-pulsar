package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogEntryBuilder;
import no.ssb.sagalog.SagaLogId;
import no.ssb.sagalog.SagaLogInitializer;
import no.ssb.sagalog.SagaLogPool;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
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

    private SagaLogPool pool;
    private SagaLog sagaLog;

    @BeforeClass
    public void createPool() {
        SagaLogInitializer initializer = new PulsarSagaLogInitializer();
        Map<String, String> configuration = initializer.configurationOptionsAndDefaults();

        String adminServiceUrl = configuration.get("pulsar.admin.service.url");
        String tenant = configuration.get("pulsar.sagalog.tenant");
        String namespace = configuration.get("pulsar.sagalog.namespace");

        ClientConfigurationData config = new ClientConfigurationData();
        config.setAuthentication(new AuthenticationDisabled());
        config.setServiceUrl(adminServiceUrl);

        try (PulsarAdmin admin = new PulsarAdmin(adminServiceUrl, config)) {
            if (!admin.tenants().getTenants().contains(tenant)) {
                admin.tenants().createTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            } else {
                admin.tenants().updateTenant(tenant, new TenantInfo(Set.of(), Set.of("standalone")));
            }
            List<String> namespaces = admin.namespaces().getNamespaces(tenant);
            if (!namespaces.contains(tenant + "/" + namespace)) {
                admin.namespaces().createNamespace(tenant + "/" + namespace, new Policies()); // TODO policies here, like retention etc.
            } else {
                // TODO update namespace policies and permissions
            }
        } catch (PulsarClientException | PulsarAdminException e) {
            throw new RuntimeException(e);
        }

        pool = initializer.initialize(configuration);
    }

    @AfterClass
    public void shutdownPool() {
        pool.shutdown();
    }

    @BeforeMethod
    private void createAndCleanPulsarSagaLog() {
        pool.release(SAGA_LOG_ID);
        sagaLog = pool.connect(SAGA_LOG_ID);
        sagaLog.truncate();
    }

    @AfterMethod
    private void closePulsarSagaLog() {
        pool.release(SAGA_LOG_ID);
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
