package com.hevodata.recoverablekafkaproducer.config;

import lombok.Getter;

@Getter
public class RecordTrackerConfig {

    public RecordTrackerConfig() {
        this.flushFrequencySecs = 5;
    }

    public RecordTrackerConfig(int flushFrequencySecs) {
        this.flushFrequencySecs = flushFrequencySecs;
    }
    private final int flushFrequencySecs;
}
