package com.hevodata.recoverablekafkaproducer.bigqueue;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.function.Function;


@AllArgsConstructor
public class RecoverableCallbackWrapper implements Callback {

    private final RecoverableCallback recoverableCallback;
    private final RecoverableRecordTracker recoverableRecordTracker;
    private final long marker;
    private final Function<Long, Boolean> failureListener;


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (null != exception) {
            handleFailure(metadata, exception);
            return;
        }
        handleSuccess(metadata);
    }

    private void handleSuccess(RecordMetadata metadata) {
        this.recoverableRecordTracker.recordFlushed(marker);
        if (recoverableCallback != null) {
            recoverableCallback.onSuccess(metadata);
        }
    }

    private void handleFailure(RecordMetadata metadata, Exception e) {
        boolean recordRecovered = this.failureListener.apply(marker);
        this.recoverableRecordTracker.recordFlushed(marker);
        if (this.recoverableCallback != null) {
            this.recoverableCallback.onFailure(metadata, e, recordRecovered);
        }
    }
}
