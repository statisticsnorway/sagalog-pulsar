package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogAlreadyAquiredByOtherOwnerException;
import no.ssb.sagalog.SagaLogId;
import no.ssb.sagalog.SagaLogOwner;
import no.ssb.sagalog.SagaLogOwnership;
import no.ssb.sagalog.SagaLogPool;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class PulsarSagaLogPool implements SagaLogPool {

    final PulsarAdmin admin;
    final PulsarClient client;
    final String tenant;
    final String namespace;
    final String instanceId;

    private final Map<SagaLogId, PulsarSagaLog> sagaLogByLogId = new ConcurrentHashMap<>();
    private final Map<SagaLogId, SagaLogOwnership> ownershipByLogId = new ConcurrentHashMap<>();

    private final Pattern sagaLogTopicPattern = Pattern.compile("(?<protocol>[^:]*)://(?<tenant>[^/]+)/(?<namespace>[^/]+)/(?<instanceId>[^:]+):(?<internalId>.+)");

    PulsarSagaLogPool(PulsarAdmin admin, PulsarClient client, String tenant, String namespace, String instanceId) {
        this.admin = admin;
        this.client = client;
        this.tenant = tenant;
        this.namespace = namespace;
        this.instanceId = instanceId;
        idFor("GhostLog"); // force early failure if pattern cannot be used with configuration
    }

    @Override
    public SagaLogId idFor(String internalId) {
        String topic = "persistent://" + tenant + "/" + namespace + "/" + instanceId + ":" + internalId;
        Matcher m = sagaLogTopicPattern.matcher(topic);
        if (!m.matches()) {
            throw new RuntimeException("topic does not match sagalog pattern: " + topic);
        }
        return new SagaLogId(topic);
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try {
            Set<SagaLogId> sagaLogIds = admin.topics().getList(tenant + "/" + namespace).stream().map(SagaLogId::new).collect(Collectors.toSet());
            return sagaLogIds;
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<SagaLogId> instanceLocalLogIds() {
        return new LinkedHashSet<>(sagaLogByLogId.keySet());
    }

    @Override
    public Set<SagaLogOwnership> instanceLocalSagaLogOwnerships() {
        return new LinkedHashSet<>(ownershipByLogId.values());
    }

    @Override
    public SagaLog connect(SagaLogId logId) {
        return sagaLogByLogId.computeIfAbsent(logId, slid -> new PulsarSagaLog(slid, client, namespace, instanceId));
    }

    @Override
    public void remove(SagaLogId logId) {
        release(logId);
        PulsarSagaLog sagaLog = sagaLogByLogId.remove(logId);
        if (sagaLog != null) {
            try {
                sagaLog.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public SagaLog acquire(SagaLogOwner owner, SagaLogId logId) throws SagaLogAlreadyAquiredByOtherOwnerException {
        SagaLog sagaLog = connect(logId);
        SagaLogOwnership ownership = ownershipByLogId.computeIfAbsent(logId, id -> new SagaLogOwnership(owner, id, ZonedDateTime.now()));
        if (!owner.equals(ownership.getOwner())) {
            throw new SagaLogAlreadyAquiredByOtherOwnerException(String.format("SagaLogOwner %s was unable to acquire saga-log with id %s. Already owned by %s.", owner.getOwnerId(), logId, ownership.getOwner().getOwnerId()));
        }
        return sagaLog;
    }

    @Override
    public void release(SagaLogOwner owner) {
        ownershipByLogId.values().removeIf(ownership -> owner.equals(ownership.getOwner()));
    }

    @Override
    public void release(SagaLogId logId) {
        ownershipByLogId.remove(logId);
    }

    @Override
    public void shutdown() {
        for (PulsarSagaLog sagaLog : sagaLogByLogId.values()) {
            try {
                sagaLog.close();
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        }
        sagaLogByLogId.clear();
        ownershipByLogId.clear();
        admin.close();
        try {
            client.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
