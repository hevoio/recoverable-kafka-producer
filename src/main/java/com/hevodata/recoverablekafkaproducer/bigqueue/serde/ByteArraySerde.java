package com.hevodata.recoverablekafkaproducer.bigqueue.serde;


import com.hevodata.recoverablekafkaproducer.bigqueue.BigQueueSerDe;

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
