package com.hevodata.recoverablekafkaproducer.commons;

@FunctionalInterface
public interface ThrowingConsumer<T> {

    void accept(T t) throws Exception;


}
