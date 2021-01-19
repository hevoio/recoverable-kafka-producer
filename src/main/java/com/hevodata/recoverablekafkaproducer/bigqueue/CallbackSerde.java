package com.hevodata.recoverablekafkaproducer.bigqueue;


public interface CallbackSerde {

    byte[] serialize(RecoverableCallback callback);

    RecoverableCallback deserialize(byte[] bytes);
}
