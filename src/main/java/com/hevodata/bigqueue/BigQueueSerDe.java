package com.hevodata.bigqueue;

import com.hevodata.exceptions.RecoveryException;

public interface BigQueueSerDe<T> {

    byte[] serialize(T t) throws RecoveryException;

    T deserialize(byte[] bytes) throws RecoveryException;

}
