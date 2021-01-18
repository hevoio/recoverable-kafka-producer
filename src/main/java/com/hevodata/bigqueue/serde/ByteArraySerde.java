package com.hevodata.bigqueue.serde;


import com.hevodata.bigqueue.BigQueueSerDe;

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
