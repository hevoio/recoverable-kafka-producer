package com.hevodata;

import com.hevodata.commons.ThrowingConsumer;
import com.hevodata.exceptions.RecoveryException;

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