package com.hevodata.recoverablekafkaproducer.bigqueue.commons;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {

    void accept(T t, U u) throws Exception;
}
