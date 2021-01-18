package com.hevodata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hevodata.commons.ThrowingBiConsumer;
import com.hevodata.exceptions.ProcessInterruptedException;
import com.hevodata.exceptions.RecoveryDisabledException;
import com.hevodata.exceptions.RecoveryException;
import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.bigqueue.IBigArray;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@SuppressWarnings("WeakerAccess")
public class BigArrayRecordStore implements RecoverableRecordStore {

    private final IBigArray bigArray;
    private final Path basePath;
    private final RecoverableRecordTracker recoverableRecordTracker;
    private static final String SHUTDOWN_MARKER = "shutdown_marker";
    private final int maxParallelism;
    private final int diskSpaceThreshold;
    private volatile boolean recoveryDisabled;
    private volatile boolean recoveryRun;
    private final ScheduledExecutorService diskSpaceMonitor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("rman-diskspace-monitor-%d").build());

    public BigArrayRecordStore(Path basePath, RecoverableRecordTracker recoverableRecordTracker,
                               int maxParallelism, int diskSpaceThreshold) throws RecoveryException {
        try {
            this.diskSpaceThreshold = diskSpaceThreshold;
            this.basePath = basePath;
            Files.createDirectories(this.basePath);
            bigArray = getBigArray(this.basePath.toString(), "data", BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
            this.recoverableRecordTracker = recoverableRecordTracker;
            this.maxParallelism = maxParallelism;
            this.recoverableRecordTracker.addMarkerFlushConsumer(marker -> {
                if (marker - this.maxParallelism > 0) {
                    this.bigArray.removeBeforeIndex(marker - this.maxParallelism);
                }

            });
            diskSpaceMonitor.scheduleAtFixedRate
                    (this::monitorDiskSpace, 1, 1, TimeUnit.MINUTES);
        } catch (IOException e) {
            log.error("Recovery manager initialization failed", e);
            throw new RecoveryException(e.getMessage());
        }
    }

    @Override
    public long publishRecord(byte[] record) throws RecoveryException {
        if (recoveryDisabled) {
            throw new RecoveryDisabledException();
        }
        try {
            return this.bigArray.append(record);
        } catch (IOException e) {
            if (e instanceof ClosedByInterruptException) {
                log.warn("Publish record to big array interrupted", e);
                throw new ProcessInterruptedException("Publish record to big array interrupted");
            } else {
                log.error("Publish record to big array failed", e);
                throw new RecoveryException(e.getMessage());
            }

        }
    }

    @Override
    public byte[] getRecord(long marker) throws RecoveryException {
        try {
            return this.bigArray.get(marker);
        } catch (IOException e) {
            log.error("Retrieving record from big array failed for marker {}", marker, e);
            throw new RecoveryException(e.getMessage());
        }
    }

    @Override
    public long consumeRecoverableRecords(ThrowingBiConsumer<Long, byte[]> recoveryRecordConsumer) throws RecoveryException {

        long totalRecords = 0;
        try {
            if (Files.exists(basePath.resolve(SHUTDOWN_MARKER))) {
                //it was shutdown gracefully last time..nothing to recover
                this.recoveryRun = true;
                return 0;
            }
            long lastFlushedMarker = this.recoverableRecordTracker.flushedTill() - maxParallelism;
            if (lastFlushedMarker < 0) {
                lastFlushedMarker = -1;
            }
            for (long i = lastFlushedMarker + 1; i < bigArray.getHeadIndex(); i++) {
                totalRecords++;
                recoveryRecordConsumer.accept(i, bigArray.get(i));
            }
        } catch (Exception e) {
            log.error("Recovery records consumption failed", e);
            throw new RecoveryException(e.getMessage());
        }
        this.recoveryRun = true;
        return totalRecords;
    }

    @Override
    public void onInitialize() throws RecoveryException {
        try {
            Files.deleteIfExists(basePath.resolve(SHUTDOWN_MARKER));
        } catch (IOException e) {
            log.error("Shutdown marker deletion failed", e);
            throw new RecoveryException(e.getMessage());
        }
    }

    private void monitorDiskSpace() {
        long totalDirSize = FileUtils.sizeOfDirectory(new File(basePath.toString())) / (1024 * 1024 * 1024);
        boolean wasRecoveryDisabled = recoveryDisabled;
        recoveryDisabled = totalDirSize > diskSpaceThreshold;
        if (!wasRecoveryDisabled && recoveryDisabled) {
            log.warn("Recovery disabled as base path {} has breached the disk threshold", basePath.toString());
        }
        if (wasRecoveryDisabled && !recoveryDisabled) {
            log.info("Recovery enabled for base path {}", basePath.toString());
        }

    }

    @Override
    public void close() throws RecoveryException {
        try {
            if (recoveryRun) {
                Files.createFile(basePath.resolve(SHUTDOWN_MARKER));
            }
            this.recoverableRecordTracker.close();
            this.diskSpaceMonitor.shutdownNow();
        } catch (Exception e) {
            log.error("Recovery manager shutdown failed", e);
            throw new RecoveryException(e.getMessage());
        }
    }

    @VisibleForTesting
    public IBigArray getBigArray(String arrayDir, String arrayName, int pageSize) throws IOException {
        return new BigArrayImpl(arrayDir, arrayName, pageSize);
    }
}
