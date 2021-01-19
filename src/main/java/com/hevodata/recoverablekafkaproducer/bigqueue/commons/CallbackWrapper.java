package com.hevodata.recoverablekafkaproducer.bigqueue.commons;

import com.hevodata.recoverablekafkaproducer.bigqueue.RecoverableCallback;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CallbackWrapper implements Callback {
    private final RecoverableCallback recoverableCallback;

    public CallbackWrapper(RecoverableCallback recoverableCallback) {
        this.recoverableCallback = recoverableCallback;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (recoverableCallback == null) {
            return;
        }
        if (null != exception) {
            recoverableCallback.onFailure(metadata, exception, false);
            return;
        }
        recoverableCallback.onSuccess(metadata);
    }
}
