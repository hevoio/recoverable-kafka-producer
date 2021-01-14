package com.hevodata;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface RecoverableCallback {

    void onSuccess(RecordMetadata metadata);

    boolean onFailure(RecordMetadata metadata, Exception e, boolean recovered);
}
