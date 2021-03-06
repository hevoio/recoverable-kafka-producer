package com.hevodata.recoverablekafkaproducer;

import com.hevodata.recoverablekafkaproducer.bigqueue.BigQueuePool;
import com.hevodata.recoverablekafkaproducer.bigqueue.BigQueuePoolConfiguration;
import com.hevodata.recoverablekafkaproducer.commons.CallbackWrapper;
import com.hevodata.recoverablekafkaproducer.commons.TimeUtils;
import com.hevodata.recoverablekafkaproducer.commons.Wrapper;
import com.hevodata.recoverablekafkaproducer.config.ProducerRecoveryConfig;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryDisabledException;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryException;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryRuntimeException;
import com.hevodata.recoverablekafkaproducer.bigqueue.serde.ByteArraySerde;
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
        RecoverableRecordTracker recoverableRecordTracker = new InMemoryKafkaCallbackRecordTracker(producerRecoveryConfig.getBaseDir(),
                producerRecoveryConfig.getRecordTrackerConfig());
        RecoverableRecordStore recoverableRecordStore = new BigArrayRecordStore(producerRecoveryConfig.getBaseDir().resolve("record_store"), recoverableRecordTracker,
                producerRecoveryConfig.getMaxParallelism(), producerRecoveryConfig.getDiskSpaceThreshold());
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
        log.info("Initializing producer {}", id);
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
        this.recoverableRecordStore.onInitialize();
    }


    @Override
    public void close() {
        try {
            this.embeddedProducer.close();
            this.recoverableRecordStore.close();
        } catch (Exception e) {
            log.error("Failed to close producer {}", id, e);
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

        int RETRIES = 3;
        for (int i = 1; i <= RETRIES; i++) {
            try {
                doCleanup();
                return;
            } catch (Exception e) {
                if (i < RETRIES) {
                    continue;
                }
                log.error("Cleanup failed for recoverable kafka producer {}", id, e);
                throw new RecoveryException(e.getMessage());
            }
        }
    }

    private void doCleanup() throws TimeoutException, IOException {
        this.bigQueuePool.awaitQueueDrain(TimeUtils.fromMinutesToMillis(5), TimeUtils.fromSecondsToMillis(1));
        long lastFailedMarkerRef = lastFailedMarker.get();
        this.embeddedProducer.flush();
        if (lastFailedMarkerRef != lastFailedMarker.get()) {
            throw new RecoveryRuntimeException("Cleanup aborted as the failed records queue is not drained");
        }
        this.bigQueuePool.cleanUp();
    }
}
