package com.hevodata;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface RecoverableCallback {

    void onSuccess(RecordMetadata metadata);

    void onFailure(RecordMetadata metadata, Exception e, boolean recovered);
}
