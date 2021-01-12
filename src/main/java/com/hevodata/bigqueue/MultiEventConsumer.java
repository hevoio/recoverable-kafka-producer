package com.hevodata.bigqueue;

import io.hevo.core.app.ShutdownFlag;
import io.hevo.core.exceptions.HevoException;
import io.hevo.core.utils.TimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes a batch of events and passes them on to the consumer.
 * Does not wait for the "success" acknowledgement from the consumer. Follows At Most Once semantics
 */
@Slf4j
public abstract class MultiEventConsumer<T> extends BaseBigQueueConsumer<T> {

    public void run() {
        while (!ShutdownFlag.get()) {
            int maxMessagesPerPoll = maxMessagesPerPoll();
            int count = consume(maxMessagesPerPoll);
            try {
                if(count < maxMessagesPerPoll) {
                    Thread.sleep(TimeUtils.fromSecondsToMillis(bigQueueConsumerConfig.getSleepTimeInSecs()));
                }
            }
            catch (InterruptedException iEx) {
                log.error("Big queue consumer interrupted temporarily");
            }
        }
        // Consume all of the remaining events
        consume(Integer.MAX_VALUE);
    }
    private int consume(int maxToPoll) {
        int count = 0;
        try {
            List<T> records = new ArrayList<>();
            while(count++ < maxToPoll && !this.bigQueue.isEmpty()) {
                byte[] bytes = this.bigQueue.dequeue();
                records.add(this.bigQueueSerDe.deserialize(bytes));
            }
            if(CollectionUtils.isNotEmpty(records)) {
                consumeValues(records);
            }
        } catch (Exception e) {
            log.error("Big queue consumer drain failed", e);
        }
        return count;
    }

    public abstract void consumeValues(List<T> values) throws HevoException;
    public abstract int maxMessagesPerPoll();
}
