package com.hevodata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hevodata.commons.Action;
import com.hevodata.commons.ThrowingConsumer;
import com.hevodata.exceptions.RecoveryException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.CollectionUtils;

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
public class InMemoryMarkersTracker implements RecoverableRecordTracker {

    private final Set<Long> pendingMarkers = Sets.newConcurrentHashSet();
    private Path baseDataDir;
    private volatile long lastMarkerSynced = -1;
    private volatile long lastMarkerBuffered = -1;
    private final List<ThrowingConsumer<Long>> markerFlushConsumers = Lists.newArrayList();
    private Action markerCleanupAction;
    private volatile long minPendingMarker = -1;
    //in milliseconds
    private long markerTTL;
    private volatile long minPendingMarkersUpdatedTs = 0;
    private static final String RECOVERY_OFFSET = "offset";


    private final ScheduledExecutorService flushService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("record-tracker-flush-%d").build());

    private final ScheduledExecutorService markerCleanupService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("record-tracker-marker-cleanup-%d").build());

    public InMemoryMarkersTracker(Path baseDataDir) {
        this.baseDataDir = baseDataDir.resolve("tracker");
        flushService.scheduleAtFixedRate(this::flushLatestMarker, 1, 1,
                TimeUnit.MINUTES);
        markerCleanupService.scheduleAtFixedRate(this::cleanUpOlderMarkers, 1, 1,
                TimeUnit.MINUTES);
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
    public void enableMarkerCleanup(long markerTTL, Action markerCleanupAction) {
        this.markerTTL = markerTTL;
        this.markerCleanupAction = markerCleanupAction;

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

    @VisibleForTesting
    public void flushLatestMarker() {
        flushLatestMarker(null);
    }

    @VisibleForTesting
    public void cleanUpOlderMarkers() {
        long minPendingMarkersCopy = minPendingMarker;
        if (minPendingMarker == -1 || this.markerCleanupAction == null ||
                (System.currentTimeMillis() - this.markerTTL < this.minPendingMarkersUpdatedTs) ||
                CollectionUtils.isEmpty(pendingMarkers)) {
            return;
        }
        log.error("Pending markers found older than {} seconds", TimeUtils.fromMillisToSeconds(markerTTL));


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
            throw new HevoRuntimeException(e.getMessage());
        }
    }

    private long markerToFlush() {

        long lastMarkerBufferClone = lastMarkerBuffered;
        try {
            long currentMinPendingMarker = Collections.min(pendingMarkers);
            if (currentMinPendingMarker != minPendingMarker) {
                this.minPendingMarkersUpdatedTs = System.currentTimeMillis();
                this.minPendingMarker = currentMinPendingMarker;
            }
            return currentMinPendingMarker - 1;
        } catch (NoSuchElementException e) {
            this.minPendingMarkersUpdatedTs = System.currentTimeMillis();
            return lastMarkerBufferClone;
        }
    }

    @Override
    public void close() {
        markerCleanupService.shutdownNow();
        flushService.shutdownNow();
        flushLatestMarker();
    }

}
