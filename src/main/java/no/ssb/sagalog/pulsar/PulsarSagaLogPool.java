package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.AbstractSagaLogPool;
import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogBusyException;
import no.ssb.sagalog.SagaLogId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Set;
import java.util.stream.Collectors;

class PulsarSagaLogPool extends AbstractSagaLogPool {

    final PulsarAdmin admin;
    final PulsarClient client;
    final String tenant;
    final String namespace;

    PulsarSagaLogPool(PulsarAdmin admin, PulsarClient client, String tenant, String namespace, String clusterInstanceId) {
        super(clusterInstanceId);
        this.admin = admin;
        this.client = client;
        this.tenant = tenant;
        this.namespace = namespace;
    }

    @Override
    public PulsarSagaLogId idFor(String clusterInstanceId, String logName) {
        return new PulsarSagaLogId(tenant, namespace, clusterInstanceId, logName);
    }

    @Override
    public Set<SagaLogId> clusterWideLogIds() {
        try {
            Set<SagaLogId> sagaLogIds = admin.topics().getList(tenant + "/" + namespace).stream().map(PulsarSagaLogId::new).collect(Collectors.toSet());
            return sagaLogIds;
        } catch (PulsarAdminException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected SagaLog connectExternal(SagaLogId logId) throws SagaLogBusyException {
        try {
            return new PulsarSagaLog(client, logId);
        } catch (PulsarClientException.ConsumerBusyException e) {
            throw new SagaLogBusyException("consumer busy - another consumer is already connected with exclusive subscription", e);
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean deleteExternal(SagaLogId logId) {
        String topic = ((PulsarSagaLogId) logId).getTopic();
        try {
            admin.topics().delete(topic);
        } catch (PulsarAdminException.NotFoundException e) {
            return true; // never existed or already deleted
        } catch (PulsarAdminException.PreconditionFailedException e) {
            return false; // active producers or consumers, unable to delete
        } catch (PulsarAdminException.NotAuthorizedException e) {
            throw new RuntimeException(String.format("Permission to delete topic %s denied", topic), e);
        } catch (PulsarAdminException e) {
            throw new RuntimeException(String.format("Error while attempting to delete topic %s", topic), e);
        }
        return true;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        admin.close();
        try {
            client.close();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }
}
