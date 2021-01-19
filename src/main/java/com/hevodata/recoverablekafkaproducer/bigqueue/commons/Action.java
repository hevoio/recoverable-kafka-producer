package com.hevodata.recoverablekafkaproducer.bigqueue.commons;

@FunctionalInterface
public interface Action {
    void execute();
}
