package com.hevodata.bigqueue;

import com.leansoft.bigqueue.IBigQueue;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class BaseBigQueueConsumer<T> {
    protected BigQueueSerDe<T> bigQueueSerDe;
    protected IBigQueue bigQueue;
    protected BigQueueConsumerConfig bigQueueConsumerConfig;

    public void initialize(BigQueueSerDe<T> bigQueueSerDe, IBigQueue bigQueue,
                           BigQueueConsumerConfig bigQueueConsumerConfig) {
        this.bigQueueSerDe = bigQueueSerDe;
        this.bigQueue = bigQueue;
        this.bigQueueConsumerConfig = bigQueueConsumerConfig;
    }

    public abstract void run();
}
