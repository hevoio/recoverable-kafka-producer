package com.hevodata.recoverablekafkaproducer.commons;

@FunctionalInterface
public interface ThrowingBiConsumer<T, U> {

    void accept(T t, U u) throws Exception;
}
