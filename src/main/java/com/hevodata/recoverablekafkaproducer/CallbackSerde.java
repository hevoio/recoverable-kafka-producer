package com.hevodata.recoverablekafkaproducer;


public interface CallbackSerde {

    byte[] serialize(RecoverableCallback callback);

    RecoverableCallback deserialize(byte[] bytes);
}
