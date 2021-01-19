package com.hevodata.recoverablekafkaproducer;

import com.hevodata.recoverablekafkaproducer.commons.ThrowingBiConsumer;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryException;

public interface RecoverableRecordStore extends AutoCloseable {

    long publishRecord(byte[] record) throws RecoveryException;

    byte[] getRecord(long marker) throws RecoveryException;

    long consumeRecoverableRecords(ThrowingBiConsumer<Long, byte[]> recoveryRecordConsumer) throws RecoveryException;

    void onInitialize() throws RecoveryException;
}
