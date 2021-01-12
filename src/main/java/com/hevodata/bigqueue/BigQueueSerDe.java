package com.hevodata.bigqueue;

import io.hevo.core.exceptions.HevoException;

public interface BigQueueSerDe<T> {

    byte[] serialize(T t) throws HevoException;

    T deserialize(byte[] bytes) throws HevoException;

}
