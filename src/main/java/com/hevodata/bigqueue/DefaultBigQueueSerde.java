package com.hevodata.bigqueue;

import com.hevodata.exceptions.RecoveryException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@AllArgsConstructor
public class DefaultBigQueueSerde<T> implements BigQueueSerDe<T> {

    private Class<T> clazz;

    @Override
    public byte[] serialize(T record) throws RecoveryException {
        return JsonUtils.objectToByteArray(record);
    }

    @Override
    public T deserialize(byte[] bytes) throws RecoveryException {
        try {
            return JsonUtils.byteArrayToObject(bytes, clazz);
        } catch (IOException e) {
            log.error("{} record deserialization failed", clazz.getSimpleName(), e);
            throw new HevoException(e.getMessage());
        }
    }
}
