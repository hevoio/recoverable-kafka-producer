package com.hevodata.bigqueue;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class BigQueuePool<T> {

    private static final String QUEUE_PREFIX = "queue-";
    private final List<IBigQueue> queues = Lists.newArrayList();
    private final ExecutorService executor;
    private final ScheduledExecutorService bigQueueGcExecutor;
    private final ScheduledExecutorService diskSpaceMonitor;
    private final BigQueuePoolConfiguration<T> bigQueuePoolConfiguration;

    private volatile boolean diskSpaceThresholdBreached = false;
    private AtomicInteger runningToken = new AtomicInteger(-1);
    private static final int TOKEN_RESET_LIMIT = 1000000;


    public BigQueuePool(BigQueuePoolConfiguration<T> bigQueuePoolConfiguration) {
        this.bigQueuePoolConfiguration = bigQueuePoolConfiguration;
        executor = Executors.newFixedThreadPool(bigQueuePoolConfiguration.getNoOfQueues(), new ThreadFactoryBuilder().setNameFormat("bigq-pool-%d").build());
        bigQueueGcExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("bigq-gc-executor-%d").build());
        bigQueueGcExecutor.scheduleAtFixedRate(this::performGc,1, 5, TimeUnit.MINUTES);
        diskSpaceMonitor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("bigq-diskspace-monitor-%d").build());
        diskSpaceMonitor.scheduleAtFixedRate(this::monitorDiskSpace, 10, 10, TimeUnit.MINUTES);
        init();
        Shutdown.registerHook(new Hook("big_queue_pool_"+ bigQueuePoolConfiguration.getName()) {
            @Override
            protected void onShutdown() {
                close(false);
            }
        });
    }

    public void publishRecord(T record) throws IOException, HevoException {
        byte[] bytes = bigQueuePoolConfiguration.getBigQueueSerDe().serialize(record);
        if (diskSpaceThresholdBreached) {
            throw new HevoRuntimeException("Buffer disk space threshold breached");
        }
        if(1 == this.queues.size()) {
            this.queues.get(0).enqueue(bytes);
            return;
        }
        int currentToken = runningToken.incrementAndGet();
        this.queues.get((currentToken % queues.size())).enqueue(bytes);
        if(runningToken.get() > TOKEN_RESET_LIMIT) {
            runningToken.set(-1);
        }
    }

    public int size() {
        int totalSize = 0;
        for (IBigQueue bigQueue : queues) {
            totalSize += bigQueue.size();

        }
        return totalSize;
    }

    private void init() {
        try {

            for (int i = 0; i < bigQueuePoolConfiguration.getNoOfQueues(); i++) {
                File queueDir = bigQueuePoolConfiguration.getBaseDir().resolve(QUEUE_PREFIX + i).toFile();
                if (!queueDir.exists()) {
                    if (!queueDir.mkdirs()) {
                        throw new HevoRuntimeException(String.format("Unable to create directory %s", queueDir.getAbsolutePath()));
                    }
                }
                IBigQueue bigQueue = new BigQueueImpl(queueDir.getAbsolutePath(), QUEUE_PREFIX + i, bigQueuePoolConfiguration.getPageSize());
                queues.add(bigQueue);

                bigQueuePoolConfiguration.getBigQueueConsumer().initialize(bigQueuePoolConfiguration.getBigQueueSerDe(), bigQueue, bigQueuePoolConfiguration.getBigQueueConsumerConfig());
                executor.execute(bigQueueConsumer::run);
            }
        }catch (Exception e){
            log.error("Big Queue Pool initialization failed", e);
            throw new HevoRuntimeException(e.getMessage());
        }
    }

    public void close(boolean force) {
        try {
            bigQueueGcExecutor.shutdownNow();
            diskSpaceMonitor.shutdownNow();
            if (force) {
                executor.shutdownNow();
            } else {
                executor.shutdown();
                try {
                    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
            }
            for (IBigQueue bigQueue : queues) {
                bigQueue.close();

            }
        } catch (IOException e) {
            log.error("Big Queue Pool cleanup failed", e);
            throw new HevoRuntimeException(e.getMessage());
        }
    }


    public void awaitQueueDrain(long timeOutMillis, long millisToSleep) throws TimeoutException {

        long startTime = System.currentTimeMillis();
        long totalSize;
        do {
            if (System.currentTimeMillis() - startTime > timeOutMillis) {
                throw new TimeoutException();
            }
            totalSize = size();
            if (totalSize > 0) {
                HevoThreadUtils.interruptIgnoredSleep(millisToSleep);
            }
        } while (totalSize > 0);
    }

    private void performGc(){
        try {
            for (IBigQueue bigQueue : queues) {
                bigQueue.gc();
            }
        }catch (IOException e){
            log.error("Gc failed for BigQueue Pool", e);
        }
    }

    public void cleanUp() throws IOException {
        for (IBigQueue bigQueue : queues) {
            bigQueue.removeAll();
        }
        close(true);
        FileUtils.deleteDirectory(bigQueuePoolConfiguration.getBaseDir().toFile());
    }

    private void monitorDiskSpace() {
        long totalDirSize = MeasurementUtils.bytesToGBs(FileUtils.sizeOfDirectory(bigQueuePoolConfiguration.getBaseDir().toFile()));
        diskSpaceThresholdBreached = totalDirSize > bigQueuePoolConfiguration.getDiskSpaceThresholdGBs();
    }
}
