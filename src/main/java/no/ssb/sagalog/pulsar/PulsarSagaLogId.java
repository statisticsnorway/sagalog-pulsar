package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLogId;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class PulsarSagaLogId implements SagaLogId {

    private static final Pattern sagaLogTopicPattern = Pattern.compile("(?<protocol>[^:]*)://(?<tenant>[^/]+)/(?<namespace>[^/]+)/(?<instanceId>[^:]+):(?<logName>.+)");

    private final String tenant;
    private final String namespace;
    private final String clusterInstanceId;
    private final String logName;

    PulsarSagaLogId(String tenant, String namespace, String clusterInstanceId, String logName) {
        if (tenant == null) {
            throw new IllegalArgumentException("tenant cannot be null");
        }
        if (namespace == null) {
            throw new IllegalArgumentException("namespace cannot be null");
        }
        if (clusterInstanceId == null) {
            throw new IllegalArgumentException("clusterInstanceId cannot be null");
        }
        if (logName == null) {
            throw new IllegalArgumentException("logName cannot be null");
        }
        this.tenant = tenant;
        this.namespace = namespace;
        this.clusterInstanceId = clusterInstanceId;
        this.logName = logName;
        String topic = getTopic();
        Matcher m = sagaLogTopicPattern.matcher(topic);
        if (!m.matches()) {
            throw new RuntimeException("topic does not match sagalog pattern: " + topic);
        }
    }

    PulsarSagaLogId(String topic) {
        Matcher m = sagaLogTopicPattern.matcher(topic);
        if (!m.matches()) {
            throw new RuntimeException("topic does not match sagalog pattern: " + topic);
        }
        this.tenant = m.group("tenant");
        this.namespace = m.group("namespace");
        this.clusterInstanceId = m.group("instanceId");
        this.logName = m.group("logName");
    }

    String getTopic() {
        return "persistent://" + tenant + "/" + namespace + "/" + clusterInstanceId + ":" + logName;
    }

    @Override
    public String getClusterInstanceId() {
        return clusterInstanceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PulsarSagaLogId that = (PulsarSagaLogId) o;
        return tenant.equals(that.tenant) &&
                namespace.equals(that.namespace) &&
                clusterInstanceId.equals(that.clusterInstanceId) &&
                logName.equals(that.logName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tenant, namespace, clusterInstanceId, logName);
    }

    @Override
    public String getLogName() {
        return logName;
    }

    @Override
    public String toString() {
        return "PulsarSagaLogId{" +
                "topic='" + getTopic() + '\'' +
                '}';
    }
}
