package com.hevodata.recoverablekafkaproducer;

import com.hevodata.recoverablekafkaproducer.commons.ThrowingConsumer;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryException;

import java.io.Closeable;

public interface RecoverableRecordTracker extends Closeable {

    void preRecordBuffering(long marker) throws RecoveryException;

    void recordBuffered(long marker) throws RecoveryException;

    void recordBufferingFailed(long marker) throws RecoveryException;

    void recordFlushed(long marker);

    void addMarkerFlushConsumer(ThrowingConsumer<Long> markerFlushConsumer);

    long flushedTill() throws RecoveryException;

    void moveMarker(long marker);
}
