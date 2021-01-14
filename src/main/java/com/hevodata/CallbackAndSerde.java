package com.hevodata;

import lombok.Data;

@Data
public class CallbackAndSerde<T extends RecoverableCallback> {
    private T callback;
    private CallbackSerde<T> callbackSerde;
}
