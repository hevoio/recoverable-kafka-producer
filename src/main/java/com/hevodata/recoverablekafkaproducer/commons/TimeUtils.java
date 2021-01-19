package com.hevodata.recoverablekafkaproducer.commons;

public class TimeUtils {

    private static final long MILLIS = 1_000L;
    private static final int SECONDS = 60;

    public static long fromMinutesToMillis(int minutes) {
        return minutes * SECONDS * MILLIS;
    }

    public static long fromSecondsToMillis(long seconds) {
        return seconds * MILLIS;
    }
}
