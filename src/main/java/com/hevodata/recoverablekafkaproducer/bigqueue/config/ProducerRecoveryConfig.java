package com.hevodata.recoverablekafkaproducer.bigqueue.config;

import com.hevodata.recoverablekafkaproducer.bigqueue.CallbackSerde;
import lombok.Builder;
import lombok.Getter;

import java.nio.file.Path;

@Getter
@Builder
public class ProducerRecoveryConfig {
    private final Path baseDir;

    @Builder.Default
    private final int maxParallelism = 100;
    //in GBs
    @Builder.Default
    private final int diskSpaceThreshold = 20;

    //this serde will be used to serialize/deserialize producer callbacks if present
    private final CallbackSerde callbackSerde;

    @Builder.Default
    private final RecordTrackerConfig recordTrackerConfig = new RecordTrackerConfig();
}
