package com.hevodata;

import com.hevodata.commons.ThrowingBiConsumer;
import com.hevodata.exceptions.RecoveryException;

public interface RecoverableRecordStore extends AutoCloseable {

    long publishRecord(byte[] record) throws RecoveryException;

    byte[] getRecord(long marker) throws RecoveryException;

    long consumeRecoverableRecords(ThrowingBiConsumer<Long, byte[]> recoveryRecordConsumer) throws RecoveryException;

    void onInitialize() throws RecoveryException;
}
