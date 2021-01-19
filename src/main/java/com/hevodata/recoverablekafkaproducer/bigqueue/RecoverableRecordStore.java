package com.hevodata.recoverablekafkaproducer.bigqueue;

import com.hevodata.recoverablekafkaproducer.bigqueue.commons.ThrowingBiConsumer;
import com.hevodata.recoverablekafkaproducer.bigqueue.exceptions.RecoveryException;

public interface RecoverableRecordStore extends AutoCloseable {

    long publishRecord(byte[] record) throws RecoveryException;

    byte[] getRecord(long marker) throws RecoveryException;

    long consumeRecoverableRecords(ThrowingBiConsumer<Long, byte[]> recoveryRecordConsumer) throws RecoveryException;

    void onInitialize() throws RecoveryException;
}
