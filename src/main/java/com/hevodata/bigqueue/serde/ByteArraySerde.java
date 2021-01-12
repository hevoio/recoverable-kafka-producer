package com.hevodata.bigqueue.serde;

import io.hevo.core.bigqueue.BigQueueSerDe;

import javax.inject.Singleton;

@Singleton
public class ByteArraySerde implements BigQueueSerDe<byte[]> {
    @Override
    public byte[] serialize(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] deserialize(byte[] bytes) {
        return bytes;
    }
}
