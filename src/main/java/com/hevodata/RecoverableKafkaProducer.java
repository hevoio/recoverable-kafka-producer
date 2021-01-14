package com.hevodata;

import com.hevodata.bigqueue.BigQueuePool;
import com.hevodata.bigqueue.BigQueuePoolConfiguration;
import com.hevodata.bigqueue.serde.ByteArraySerde;
import com.hevodata.commons.CallbackWrapper;
import com.hevodata.commons.TimeUtils;
import com.hevodata.commons.Wrapper;
import com.hevodata.exceptions.RecoveryDisabledException;
import com.hevodata.exceptions.RecoveryException;
import com.hevodata.exceptions.RecoveryRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RecoverableKafkaProducer implements Closeable {

    private KafkaProducer<byte[], byte[]> embeddedProducer;
    private RecoverableRecordStore recoverableRecordStore;
    private RecoverableRecordTracker recoverableRecordTracker;
    private final RecoverableProducerRecordSerde recoverableProducerRecordSerde;
    private BigQueuePool<byte[]> bigQueuePool;
    private final Wrapper<Long> lastFailedMarker = Wrapper.of(-1L);
    private ProducerRecoveryConfig producerRecoveryConfig;
    private String id;

    public RecoverableKafkaProducer(KafkaProducer<byte[], byte[]> embeddedProducer, ProducerRecoveryConfig producerRecoveryConfig) throws RecoveryException {
        RecoverableRecordTracker recoverableRecordTracker = new InMemoryRecordTracker(producerRecoveryConfig.getBaseDir().resolve("tracker"));
        RecoverableRecordStore recoverableRecordStore = new BigArrayRecordStore(producerRecoveryConfig.getBaseDir().resolve("data"), recoverableRecordTracker,
                producerRecoveryConfig.getMaxParallelism(), producerRecoveryConfig.getDiskSpaceThreshold());
        this.recoverableProducerRecordSerde = new RecoverableProducerRecordSerde(producerRecoveryConfig.getCallbackSerde());
        doInitializeProducer(embeddedProducer, producerRecoveryConfig, recoverableRecordStore, recoverableRecordTracker);
    }

    public RecoverableKafkaProducer(KafkaProducer<byte[], byte[]> embeddedProducer, ProducerRecoveryConfig producerRecoveryConfig,
                                    RecoverableRecordStore recoverableRecordStore, RecoverableRecordTracker recoverableRecordTracker) throws RecoveryException {
        this.recoverableProducerRecordSerde = new RecoverableProducerRecordSerde(producerRecoveryConfig.getCallbackSerde());
        doInitializeProducer(embeddedProducer, producerRecoveryConfig, recoverableRecordStore, recoverableRecordTracker);

    }

    private void doInitializeProducer(KafkaProducer<byte[], byte[]> embeddedProducer, ProducerRecoveryConfig producerRecoveryConfig,
                                      RecoverableRecordStore recoverableRecordStore, RecoverableRecordTracker recoverableRecordTracker) throws RecoveryException {
        this.embeddedProducer = embeddedProducer;
        this.recoverableRecordTracker = recoverableRecordTracker;
        this.recoverableRecordStore = recoverableRecordStore;
        this.producerRecoveryConfig = producerRecoveryConfig;
        this.id = UUID.randomUUID().toString();
        initializeFailedRecordsQueue();
        consumeRecoverableRecords();

    }

    private void initializeFailedRecordsQueue() {
        String poolName = "recoverable-producer-" + id;
        this.bigQueuePool = new BigQueuePool<>(BigQueuePoolConfiguration.<byte[]>builder()
                .name(poolName).noOfQueues(1).baseDir(producerRecoveryConfig.getBaseDir().resolve("failed_records"))
                .bigQueueConsumer(new KafkaFailedRecordsConsumer(this)).bigQueueSerDe(new ByteArraySerde()).diskSpaceThresholdGBs(2)
                .build());
    }


    public void publish(ProducerRecord<byte[], byte[]> record) throws RecoveryException {
        publish(record, null);
    }

    public void publish(ProducerRecord<byte[], byte[]> record, RecoverableCallback recoverableCallback) throws RecoveryException {
        byte[] recoveryRecordBytes = this.recoverableProducerRecordSerde.serialize(buildProducerRecoveryRecord(record, recoverableCallback));

        try {
            long marker = this.recoverableRecordStore.publishRecord(recoveryRecordBytes);
            this.recoverableRecordTracker.preRecordBuffering(marker);
            try {
                this.embeddedProducer.send(record, new RecoverableCallbackWrapper(recoverableCallback, this.recoverableRecordTracker,
                        marker, markerRef -> {
                    try {
                        bigQueuePool.publishRecord(this.recoverableRecordStore.getRecord(markerRef));
                        lastFailedMarker.setData(markerRef);
                    } catch (Exception e) {
                        log.error("Publishing failed record to big queue failed for marker {}", markerRef, e);
                        return false;
                    }
                    return true;
                }));

            } catch (Exception e) {
                this.recoverableRecordTracker.recordBufferingFailed(marker);
                throw e;
            }
            this.recoverableRecordTracker.recordBuffered(marker);
        } catch (RecoveryDisabledException e) {
            log.warn("Recovery disabled for producer {}", id);
        }
    }

    private RecoverableProducerRecord buildProducerRecoveryRecord(ProducerRecord<byte[], byte[]> record, RecoverableCallback recoverableCallback) {
        return RecoverableProducerRecord.builder().topic(record.topic())
                .partition(record.partition()).key(record.key()).message(record.value())
                .recoverableCallback(recoverableCallback).build();

    }

    private void consumeRecoverableRecords() throws RecoveryException {
        Wrapper<Long> lastConsumedMarker = new Wrapper<>();
        long totalRecordsRecovered = this.recoverableRecordStore.consumeRecoverableRecords((marker, recoveryRecord) -> {
            RecoverableProducerRecord recoverableProducerRecord = this.recoverableProducerRecordSerde.deserialize(recoveryRecord);
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(recoverableProducerRecord.getTopic(),
                            recoverableProducerRecord.getKey(), recoverableProducerRecord.getMessage());
            this.embeddedProducer.send(producerRecord, new CallbackWrapper(recoverableProducerRecord.getRecoverableCallback()));
            lastConsumedMarker.setData(marker);
        });
        if (totalRecordsRecovered > 0) {
            this.embeddedProducer.flush();
        }
        log.info("{} records recovered", totalRecordsRecovered);
        if (lastConsumedMarker.get() != null) {
            this.recoverableRecordTracker.moveMarker(lastConsumedMarker.get());
        }
        this.recoverableRecordStore.markInitialized();
    }


    @Override
    public void close() {
        try {
            this.embeddedProducer.close();
            this.recoverableRecordStore.close();
        } catch (Exception e) {
            log.error("Recoverable kafka producer close failed", e);
            throw new RecoveryRuntimeException(e.getMessage());
        }
    }

    public void republishRecoveryRecord(byte[] recoveryRecord) throws RecoveryException, IOException {
        RecoverableProducerRecord recoverableProducerRecord = this.recoverableProducerRecordSerde.deserialize(recoveryRecord);
        ProducerRecord<byte[], byte[]> producerRecord =
                new ProducerRecord<>(recoverableProducerRecord.getTopic(),
                        recoverableProducerRecord.getKey(), recoverableProducerRecord.getMessage());

        this.publish(producerRecord, recoverableProducerRecord.getRecoverableCallback());
    }

    public void cleanUp() throws RecoveryException {
        try {
            doCleanup();
        } catch (Exception e) {
            log.error("Cleanup failed for recoverable kafka producer {}", id, e);
            throw new RecoveryException(e.getMessage());
        }
    }

    private void doCleanup() throws TimeoutException, IOException {
        this.bigQueuePool.awaitQueueDrain(TimeUtils.fromMinutesToMillis(5), TimeUtils.fromSecondsToMillis(1));
        long lastFailedMarkerRef = lastFailedMarker.get();
        this.embeddedProducer.flush();
        if (lastFailedMarkerRef != lastFailedMarker.get()) {
            throw new RecoveryRuntimeException("Failed records queue not empty");
        }
        this.bigQueuePool.cleanUp();
    }
}
