package com.hevodata;

import com.google.common.collect.Lists;
import com.hevodata.bigqueue.BigQueuePool;
import com.hevodata.commons.TimeUtils;
import com.hevodata.commons.Wrapper;
import com.hevodata.exceptions.RecoveryException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RecoverableKafkaProducer {

    private KafkaProducer<byte[], byte[]> embeddedProducer;
    private RecoverableRecordStore recoverableRecordStore;
    private RecoverableRecordTracker recoverableRecordTracker;
    private RecoverableProducerRecordSerde recoverableProducerRecordSerde;
    private BigQueuePool<byte[]> bigQueuePool;
    private final Wrapper<Long> lastFailedMarker = Wrapper.of(-1L);
    private ProducerRecoveryConfig producerRecoveryConfig;

    public RecoverableKafkaProducer(KafkaProducer<byte[], byte[]> embeddedProducer, ProducerRecoveryConfig producerRecoveryConfig) throws RecoveryException {
        RecoverableRecordTracker recoverableRecordTracker = new InMemoryMarkersTracker(producerRecoveryConfig.getBaseDir().resolve("tracker"));
        RecoverableRecordStore recoverableRecordStore = new BigArrayRecordStore(producerRecoveryConfig.getBaseDir().resolve("data"), recoverableRecordTracker,
                producerRecoveryConfig.getMaxParallelism(), producerRecoveryConfig.getDiskSpaceThreshold());
        this(embeddedProducer, producerRecoveryConfig, recoverableRecordStore, recoverableRecordTracker)


    }

    public RecoverableKafkaProducer(KafkaProducer<byte[], byte[]> embeddedProducer, ProducerRecoveryConfig producerRecoveryConfig,
                                    RecoverableRecordStore recoverableRecordStore, RecoverableRecordTracker recoverableRecordTracker) throws RecoveryException {

        this.embeddedProducer = embeddedProducer;
        this.recoverableRecordTracker = recoverableRecordTracker;
        this.recoverableRecordStore = recoverableRecordStore;
        initializeFailedRecordsQueue();

        this.recoverableRecordTracker.enableMarkerCleanup(TimeUtils.fromMinutesToMillis(10), () -> {
            this.embeddedProducer.flush();
        });

        Shutdown.registerHook(new Hook(Hook.HookType.PRODUCER, "Recoverable-kafka-producer:" + producerId()) {
            @Override
            protected void onShutdown() {
                close();
            }
        });
    }

    private void initializeFailedRecordsQueue() {
        String poolName = "producer-recovery-" + producerId();
        ByteArraySerde byteArraySerde = Registry.getType(ByteArraySerde.class);
        this.bigQueuePool = new BigQueuePool<>(BigQueuePoolConfiguration.<byte[]>builder()
                .name(poolName).noOfQueues(1).baseDir(Paths.get(FileUtils.DATA_DIRECTORY, poolName))
                .bigQueueConsumerType(KafkaFailedRecordsConsumer.class).bigQueueSerDe(byteArraySerde).diskSpaceThresholdGBs(2)
                .build());
    }


    @Override
    public void publish(ProducerRecord<String, byte[]> record) throws HevoException {
        publish(record, null);
    }

    @Override
    public void publish(ProducerRecord<String, byte[]> record, HevoRecordCallback recordCallback) throws HevoException {
        publish(record, recordCallback, Lists.newArrayList());
    }

    @Override
    public void publish(ProducerRecord<String, byte[]> record, HevoRecordCallback recordCallback, List<HevoRecordTag> recordTags) throws HevoException {
        byte[] recoveryRecordBytes = this.recoverableProducerRecordSerde.serialize(buildProducerRecoveryRecord(record,
                recordCallback, recordTags));

        if (!this.recoverableRecordStore.recoveryEnabled()) {
            this.embeddedProducer.publish(record, recordCallback, recordTags);
            return;
        }
        try {
            long marker = this.hevoRecoveryManager.publishRecord(recoveryRecordBytes);

            this.recoverableRecordTracker.preRecordBuffering(marker);
            try {
                this.hevoKafkaProducer.publish(record, new RecoverableRecordTrackerCallback(recordCallback,
                                recoverableRecordTracker, marker, markerRef ->
                        {
                            try {
                                bigQueuePool.publishRecord(this.hevoRecoveryManager.getRecord(markerRef));
                                lastFailedMarker.setData(markerRef);
                            } catch (Exception e) {
                                log.error("Publishing failed record to big queue failed for marker {}", markerRef, e);
                                return false;
                            }
                            return true;
                        }),
                        recordTags);
            } catch (Exception e) {
                this.recoverableRecordTracker.recordBufferingFailed(marker);
                throw e;
            }
            this.recoverableRecordTracker.recordBuffered(marker);
        } catch (RecoveryDisabledException e) {
            log.warn("Recovery disabled for producer {}", producerId());
        }

    }


    private RecoverableProducerRecord buildProducerRecoveryRecord(ProducerRecord<String, byte[]> record, HevoRecordCallback recordCallback, List<HevoRecordTag> recordTags) {
        return RecoverableProducerRecord.builder().topic(record.topic())
                .partition(record.partition()).key(record.key()).message(record.value())
                .tags(recordTags).hevoRecordCallback(recordCallback).build();

    }

    public void consumeRecoverableRecords() throws RecoveryException {
        this.recoverableRecordStore.setRecoverable(false);
        Wrapper<Long> lastConsumedMarker = new Wrapper<>();
        long totalRecordsRecovered = this.recoverableRecordStore.consumeRecoverableRecords((marker, recoveryRecord) -> {
            republishRecoveryRecord(recoveryRecord);
            lastConsumedMarker.setData(marker);
        });
        if (totalRecordsRecovered > 0) {
            this.embeddedProducer.flush();
        }
        log.info("{} records recovered", totalRecordsRecovered);
        if (lastConsumedMarker.get() != null) {
            this.recoverableRecordTracker.moveMarker(lastConsumedMarker.get());
        }
        this.recoverableRecordStore.setRecoverable(true);
        this.recoverableRecordStore.markInitialized();

    }


    public void close() {
        try {
            this.embeddedProducer.close();
            this.recoverableRecordStore.close();
        } catch (Exception e) {
            log.error("Recoverable kafka producer close failed", e);
            throw new HevoRuntimeException(e.getMessage());
        }
    }

    public void republishRecoveryRecord(byte[] recoveryRecord) throws RecoveryException, IOException {
        RecoverableProducerRecord recoverableProducerRecord = this.recoverableProducerRecordSerde.deserialize(recoveryRecord);
        ProducerRecord<String, byte[]> producerRecord =
                new ProducerRecord<>(recoverableProducerRecord.getTopic(),
                        recoverableProducerRecord.getKey(), recoverableProducerRecord.getMessage());

        this.publish(producerRecord, recoverableProducerRecord.getHevoRecordCallback(),
                recoverableProducerRecord.getTags());
    }

    public void cleanUp() throws RecoveryException {
        try {
            log.info("Cleanup started for recoverable kafka producer {}", producerId());
            doCleanup();
            log.info("Cleanup succeeded for recoverable kafka producer {}", producerId());
        } catch (Exception e) {
            log.error("Cleanup failed for recoverable kafka producer {}", producerId(), e);
            throw new RecoveryException(e.getMessage());
        }
    }

    @Retry(attempts = 3, waitTime = 100)
    private void doCleanup() throws TimeoutException, IOException {
        this.bigQueuePool.awaitQueueDrain(TimeUtils.fromMinutesToMillis(5), TimeUtils.fromSecondsToMillis(1));
        long lastFailedMarkerRef = lastFailedMarker.get();
        this.flush();
        if (lastFailedMarkerRef != lastFailedMarker.get()) {
            throw new HevoRuntimeException("Failed records queue not empty");
        }
        this.bigQueuePool.cleanUp();
    }
}
