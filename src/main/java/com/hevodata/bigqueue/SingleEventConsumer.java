package com.hevodata.bigqueue;

import com.hevodata.commons.TimeUtils;
import com.hevodata.commons.Utils;
import com.hevodata.exceptions.RecoveryException;
import com.leansoft.bigqueue.IBigQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * Processes one event at a time and follows the At least once semantics
 */
@Slf4j
public abstract class SingleEventConsumer<T> implements BigQueueConsumer<T> {

    private volatile boolean shutdown;

    @Override
    public void run(BigQueueSerDe<T> bigQueueSerDe, IBigQueue bigQueue, BigQueueConsumerConfig bigQueueConsumerConfig) {

        while (!shutdown) {
            try {
                byte[] bytes = bigQueue.peek();
                if (bytes == null) {
                    Utils.interruptIgnoredSleep(TimeUtils.fromSecondsToMillis(bigQueueConsumerConfig.getSleepTimeInSecs()));
                    continue;
                }
                T value = bigQueueSerDe.deserialize(bytes);
                consumeValue(value);
                bigQueue.dequeue();

            } catch (Exception e) {
                log.error("Big queue consumer poll failed", e);
                Utils.interruptIgnoredSleep(1000);
            }
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    public abstract void consumeValue(T value) throws RecoveryException;
}
