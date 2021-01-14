package com.hevodata;

import com.hevodata.commons.ThrowingConsumer;
import com.hevodata.exceptions.RecoveryException;

public interface RecoverableRecordTracker extends AutoCloseable {

    void preRecordBuffering(long marker) throws RecoveryException;

    void recordBuffered(long marker) throws RecoveryException;

    void recordBufferingFailed(long marker) throws RecoveryException;

    void recordFlushed(long marker);

    void addMarkerFlushConsumer(ThrowingConsumer<Long> markerFlushConsumer);

    long flushedTill() throws RecoveryException;

    void moveMarker(long marker);
}