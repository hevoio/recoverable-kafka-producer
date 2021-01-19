package com.hevodata.recoverablekafkaproducer.bigqueue;

import com.leansoft.bigqueue.IBigQueue;

public interface BigQueueConsumer<T> {

    void run(BigQueueSerDe<T> bigQueueSerDe, IBigQueue bigQueue, BigQueueConsumerConfig bigQueueConsumerConfig);

    void shutdown();
}
