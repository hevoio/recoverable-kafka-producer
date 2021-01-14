package com.hevodata;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class RecoverableProducerRecord {
    private String topic;
    private Integer partition;
    private byte[] key;
    private byte[] message;
    private RecoverableCallback recoverableCallback;

}
