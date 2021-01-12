package com.hevodata;

import io.hevo.core.kafka.HevoRecordCallback;
import io.hevo.core.kafka.HevoRecordTag;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.producer.Callback;

import java.util.List;

@Builder
@Data
public class RecoverableProducerRecord {
    private String topic;
    private Integer partition;
    private String key;
    private byte[] message;
    private Callback callback;
}
