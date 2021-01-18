package com.hevodata;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hevodata.commons.ThrowingConsumer;
import com.hevodata.config.RecordTrackerConfig;
import com.hevodata.exceptions.RecoveryException;
import com.hevodata.exceptions.RecoveryRuntimeException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


@Slf4j
@SuppressWarnings("WeakerAccess")
public class InMemoryKafkaCallbackRecordTracker implements RecoverableRecordTracker {

    private final Set<Long> pendingMarkers = Sets.newConcurrentHashSet();
    private final Path baseDataDir;
    private volatile long lastMarkerSynced = -1;
    private volatile long lastMarkerBuffered = -1;
    private final List<ThrowingConsumer<Long>> markerFlushConsumers = Lists.newArrayList();
    private static final String RECOVERY_OFFSET = "offset";


    private final ScheduledExecutorService flushService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("record-tracker-flush-%d").build());


    public InMemoryKafkaCallbackRecordTracker(Path baseDataDir, RecordTrackerConfig recordTrackerConfig) {
        this.baseDataDir = baseDataDir.resolve("tracker");
        flushService.scheduleAtFixedRate(this::flushLatestMarker, recordTrackerConfig.getInitialFlushDelaySecs(),
                recordTrackerConfig.getFlushFrequencySecs(), TimeUnit.SECONDS);
    }

    @Override
    public void preRecordBuffering(long marker) {
        pendingMarkers.add(marker);
    }

    @Override
    public void recordBuffered(long marker) {
        lastMarkerBuffered = marker;
    }

    @Override
    public void recordBufferingFailed(long marker) {
        pendingMarkers.remove(marker);
    }

    @Override
    public void recordFlushed(long marker) {
        pendingMarkers.remove(marker);
    }

    @Override
    public void addMarkerFlushConsumer(ThrowingConsumer<Long> markerFlushConsumer) {
        this.markerFlushConsumers.add(markerFlushConsumer);
    }

    @Override
    public long flushedTill() throws RecoveryException {
        try {
            if (Files.exists(baseDataDir.resolve(RECOVERY_OFFSET))) {
                return Long.parseLong(Files.readAllLines(baseDataDir.resolve(RECOVERY_OFFSET)).get(0));
            }
        } catch (IOException e) {
            log.error("Flush position fetch failed ", e);
            throw new RecoveryException(e.getMessage());
        }
        return -1;
    }

    @Override
    public void moveMarker(long marker) {
        flushLatestMarker(marker);
    }

    public void flushLatestMarker() {
        flushLatestMarker(null);
    }

    private void flushLatestMarker(Long marker) {
        try {
            Path tempFilePath = baseDataDir.resolve(RECOVERY_OFFSET + "_temp");
            long markerToFlush = marker != null ? marker : markerToFlush();
            if (markerToFlush == -1 || markerToFlush == lastMarkerSynced) {
                return;
            }

            Files.createDirectories(tempFilePath.getParent());
            Files.write(tempFilePath, String.valueOf(markerToFlush).getBytes());
            Files.move(tempFilePath, baseDataDir.resolve(RECOVERY_OFFSET),
                    StandardCopyOption.ATOMIC_MOVE);
            lastMarkerSynced = markerToFlush;
            for (ThrowingConsumer<Long> markerFlushConsumer : markerFlushConsumers) {
                markerFlushConsumer.accept(markerToFlush);
            }
        } catch (Exception e) {
            log.error("Recovery refs flush failed", e);
            throw new RecoveryRuntimeException(e.getMessage());
        }
    }

    private long markerToFlush() {

        long lastMarkerBufferClone = lastMarkerBuffered;
        try {
            long currentMinPendingMarker = Collections.min(pendingMarkers);
            return currentMinPendingMarker - 1;
        } catch (NoSuchElementException e) {
            return lastMarkerBufferClone;
        }
    }

    @Override
    public void close() {
        flushService.shutdownNow();
        flushLatestMarker();
    }

}
