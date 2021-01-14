package com.hevodata.commons;

public class Utils {

    public static void interruptIgnoredSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static long bytesToMBs(long bytes) {
        return bytes / (1024 * 1024);
    }

    public static long bytesToGBs(long bytes) {
        return bytesToMBs(bytes) / 1024;
    }
}
