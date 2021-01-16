package com.hevodata.config;

import lombok.Getter;

@Getter
public class RecordTrackerConfig {

    public RecordTrackerConfig() {
        this.initialFlushDelaySecs = 5;
        this.flushFrequencySecs = 5;
    }

    public RecordTrackerConfig(int initialFlushDelaySecs, int flushFrequencySecs) {
        this.initialFlushDelaySecs = initialFlushDelaySecs;
        this.flushFrequencySecs = flushFrequencySecs;
    }

    private final int initialFlushDelaySecs;
    private final int flushFrequencySecs;
}
