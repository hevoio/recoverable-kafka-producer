package com.hevodata;

import com.hevodata.bigqueue.SingleEventConsumer;
import com.hevodata.exceptions.RecoveryException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class KafkaFailedRecordsConsumer extends SingleEventConsumer<byte[]> {

    private RecoverableKafkaProducer recoverableKafkaProducer;

    public KafkaFailedRecordsConsumer(RecoverableKafkaProducer recoverableKafkaProducer) {
        this.recoverableKafkaProducer = recoverableKafkaProducer;

    }

    @Override
    public void consumeValue(byte[] recoveryRecord) throws RecoveryException {
        try {
            recoverableKafkaProducer.republishRecoveryRecord(recoveryRecord);
        } catch (IOException e) {
            log.error("Kafka failed records republish failed", e);
            throw new RecoveryException(e.getMessage());
        }
    }
}
