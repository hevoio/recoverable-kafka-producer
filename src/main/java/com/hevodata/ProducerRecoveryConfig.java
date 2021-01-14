package com.hevodata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.file.Path;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProducerRecoveryConfig {
    private Path baseDir;
    private int maxParallelism = 1000;
    //in GBs
    private int diskSpaceThreshold = 20;

    //this serde will be used to serialize/deserialize producer callbacks if present
    private CallbackSerde callbackSerde;
}
