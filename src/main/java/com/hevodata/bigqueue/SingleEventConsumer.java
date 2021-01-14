package com.hevodata.bigqueue;
import com.hevodata.exceptions.RecoveryException;
import lombok.extern.slf4j.Slf4j;

/**
 * Processes one event at a time and follows the At least once semantics
 */
@Slf4j
public abstract class SingleEventConsumer<T> extends BaseBigQueueConsumer<T> {

    @Override
    public void run() {

        while (!ShutdownFlag.get()) {
            try {
                byte[] bytes = this.bigQueue.peek();
                if (bytes == null) {
                    Thread.sleep(TimeUtils.fromSecondsToMillis(bigQueueConsumerConfig.getSleepTimeInSecs()));
                    continue;
                }
                T value = this.bigQueueSerDe.deserialize(bytes);
                consumeValue(value);
                this.bigQueue.dequeue();

            } catch (Exception e) {
                logException(e);
                try {
                    Thread.sleep(TimeUtils.fromSecondsToMillis(bigQueueConsumerConfig.getSleepTimeInSecs()));
                } catch (InterruptedException e1) {
                    //ignore
                }
            }
        }
    }

    private void logException(Exception e) {
        if (!HevoExceptionUtils.exceptionContainsMessage(e, "sleep interrupted")) {
            log.error("Big queue consumer poll failed", e);
        }
    }

    public abstract void consumeValue(T value) throws RecoveryException;
}
