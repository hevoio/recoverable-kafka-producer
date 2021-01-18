package com.hevodata.bigqueue;

import com.leansoft.bigqueue.IBigQueue;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;

public interface BigQueueConsumer<T> {

    void run(BigQueueSerDe<T> bigQueueSerDe, IBigQueue bigQueue, BigQueueConsumerConfig bigQueueConsumerConfig);

    void shutdown();
}