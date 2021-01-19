package com.hevodata.recoverablekafkaproducer.bigqueue;

import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryException;

public interface BigQueueSerDe<T> {

    byte[] serialize(T t) throws RecoveryException;

    T deserialize(byte[] bytes) throws RecoveryException;

}
