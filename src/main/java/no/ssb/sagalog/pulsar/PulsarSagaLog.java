package no.ssb.sagalog.pulsar;

import no.ssb.sagalog.SagaLog;
import no.ssb.sagalog.SagaLogEntry;
import no.ssb.sagalog.SagaLogEntryBuilder;
import no.ssb.sagalog.SagaLogEntryId;
import no.ssb.sagalog.SagaLogEntryType;
import no.ssb.sagalog.SagaLogId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.shade.com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

class PulsarSagaLog implements SagaLog, AutoCloseable {

    private final PulsarSagaLogId sagaLogId;

    private final PulsarAdmin admin;
    private final Producer<byte[]> producer;
    private final Consumer<byte[]> consumer;

    private final Deque<SagaLogEntry> cache = new ConcurrentLinkedDeque<>();

    PulsarSagaLog(PulsarAdmin admin, SagaLogId _sagaLogId, PulsarClient client, String namespace, String clusterInstanceId) throws PulsarClientException, PulsarAdminException {
        this.admin = admin;
        this.sagaLogId = (PulsarSagaLogId) _sagaLogId;
        this.consumer = client.newConsumer()
                .topic(sagaLogId.getTopic())
                .subscriptionType(SubscriptionType.Exclusive)
                .consumerName(namespace + "::" + clusterInstanceId)
                .subscriptionName("master")
                .subscribe();
        this.producer = client.newProducer()
                .topic(sagaLogId.getTopic())
                .producerName(namespace + "::" + clusterInstanceId)
                .create();

        JsonObject internalInfo = admin.topics().getInternalInfo(sagaLogId.getTopic());

        readExternal().forEachOrdered(entry -> cache.add(entry));
    }

    @Override
    public SagaLogId id() {
        return sagaLogId;
    }

    private Stream<SagaLogEntry> readExternal() {
        // produce a new control message to indicate "end-of-stream"
        CompletableFuture lastMessageIdCompletableFuture = producer.sendAsync(serialize(builder().control()));

        Iterator<SagaLogEntry> iterator = new Iterator<>() {
            MessageId previousmessageId = null;

            @Override
            public boolean hasNext() {
                return !lastMessageIdCompletableFuture.join().equals(previousmessageId);
            }

            @Override
            public SagaLogEntry next() {
                try {
                    Message<byte[]> message;
                    while ((message = consumer.receive(3, TimeUnit.SECONDS)) == null) ;
                    previousmessageId = message.getMessageId();
                    return deserialize(message.getData()).id(new PulsarSagaLogEntryId(message.getMessageId())).build();
                } catch (PulsarClientException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL),
                false)
                .filter(entry -> SagaLogEntryType.Ignore != entry.getEntryType()); // remove control messages
    }

    @Override
    public CompletableFuture<SagaLogEntry> write(SagaLogEntryBuilder builder) {
        return producer.sendAsync(serialize(builder)).thenApply(messageId -> {
            SagaLogEntry entry = builder.id(new PulsarSagaLogEntryId(messageId)).build();
            cache.add(entry);
            return entry;
        });
    }

    @Override
    public CompletableFuture<Void> truncate(SagaLogEntryId messageId) {
        PulsarSagaLogEntryId pulsarMessageId = (PulsarSagaLogEntryId) messageId;
        // check that message-id is in cache, if not, throw exception
        cache.stream().filter(entry -> messageId.equals(entry.getId())).findFirst().orElseThrow();
        return consumer.acknowledgeCumulativeAsync(pulsarMessageId.id).thenAccept(v -> {
            Iterator<SagaLogEntry> iterator = cache.iterator();
            while (iterator.hasNext()) {
                SagaLogEntry entry = iterator.next();
                iterator.remove();
                if (messageId.equals(entry.getId())) {
                    return;
                }
            }
        });
    }

    @Override
    public CompletableFuture<Void> truncate() {
        if (cache.isEmpty()) {
            return CompletableFuture.completedFuture(null); // nothing to truncate
        }
        return truncate(cache.getLast().getId());
    }

    @Override
    public Stream<SagaLogEntry> readIncompleteSagas() {
        return cache.stream();
    }

    @Override
    public Stream<SagaLogEntry> readEntries(String executionId) {
        return cache.stream().filter(entry -> executionId.equals(entry.getExecutionId()));
    }

    @Override
    public String toString(SagaLogEntryId id) {
        return ((PulsarSagaLogEntryId) id).id.toString();
    }

    @Override
    public SagaLogEntryId fromString(String id) {
        String[] parts = id.split(":");
        long ledgerId = Long.parseLong(parts[0]);
        long entryId = Long.parseLong(parts[1]);
        int partitionIndex = Integer.parseInt(parts[2]);
        return new PulsarSagaLogEntryId(DefaultImplementation.newMessageId(ledgerId, entryId, partitionIndex));
    }

    @Override
    public byte[] toBytes(SagaLogEntryId id) {
        return ((PulsarSagaLogEntryId) id).id.toByteArray();
    }

    @Override
    public SagaLogEntryId fromBytes(byte[] idBytes) {
        try {
            return new PulsarSagaLogEntryId(MessageIdImpl.fromByteArray(idBytes));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    byte[] serialize(SagaLogEntryBuilder builder) {
        String serializedString = builder.executionId()
                + " " + builder.entryType()
                + " " + builder.nodeId()
                + (builder.sagaName() == null ? "" : " " + builder.sagaName())
                + (builder.jsonData() == null ? "" : " " + builder.jsonData());
        return serializedString.getBytes(StandardCharsets.UTF_8);
    }

    SagaLogEntryBuilder deserialize(byte[] bytes) {
        String serialized = new String(bytes, StandardCharsets.UTF_8);
        SagaLogEntryBuilder builder = builder();

        // mandatory log-fields

        int executionIdEndIndex = serialized.indexOf(' ');
        String executionId = serialized.substring(0, executionIdEndIndex);
        serialized = serialized.substring(executionIdEndIndex + 1);

        builder.executionId(executionId);

        int entryTypeEndIndex = serialized.indexOf(' ');
        SagaLogEntryType entryType = SagaLogEntryType.valueOf(serialized.substring(0, entryTypeEndIndex));
        serialized = serialized.substring(entryTypeEndIndex + 1);

        builder.entryType(entryType);

        int nodeIdEndIdex = serialized.indexOf(' ');
        if (nodeIdEndIdex == -1) {
            return builder.nodeId(serialized);
        }

        String nodeId = serialized.substring(0, nodeIdEndIdex);
        serialized = serialized.substring(nodeIdEndIdex + 1);

        builder.nodeId(nodeId);

        // optional log-fields
        if ("S".equals(nodeId)) {
            // Start nodeId
            int jsonDataBeginIndex = serialized.indexOf('{');
            if (jsonDataBeginIndex == -1) {
                String sagaName = serialized.substring(0, serialized.length() - 1);
                return builder.sagaName(sagaName);
            }
            String sagaName = serialized.substring(0, jsonDataBeginIndex - 1);
            String jsonData = serialized.substring(jsonDataBeginIndex);
            return builder.sagaName(sagaName).jsonData(jsonData);
        }

        int jsonDataBeginIndex = serialized.indexOf('{');
        if (jsonDataBeginIndex == -1) {
            return builder;
        }
        String jsonData = serialized.substring(jsonDataBeginIndex);
        return builder.jsonData(jsonData);
    }

    @Override
    public void close() throws PulsarClientException {
        try {
            producer.close();
        } finally {
            consumer.close();
        }
    }
}
