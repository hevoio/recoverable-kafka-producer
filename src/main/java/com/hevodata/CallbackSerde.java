package com.hevodata;


public interface CallbackSerde {

    byte[] serialize(RecoverableCallback callback);

    RecoverableCallback deserialize(byte[] bytes);
}
