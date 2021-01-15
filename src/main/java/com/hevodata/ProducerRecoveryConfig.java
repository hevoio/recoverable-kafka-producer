package com.hevodata;

import lombok.*;

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
}
