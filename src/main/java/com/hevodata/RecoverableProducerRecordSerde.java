package com.hevodata;

import com.hevodata.commons.ByteBufferUtils;
import com.hevodata.exceptions.RecoveryException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static com.hevodata.commons.ByteBufferUtils.SIZE_OF_INT;


@SuppressWarnings("unchecked")
public class RecoverableProducerRecordSerde {

    private final CallbackSerde callbackSerde;

    public RecoverableProducerRecordSerde(CallbackSerde callbackSerde) {
        this.callbackSerde = callbackSerde;
    }

    public byte[] serialize(RecoverableProducerRecord recoverableProducerRecord) throws RecoveryException {

        RecoverableCallback callback = recoverableProducerRecord.getRecoverableCallback();
        byte[] recordCallbackBytes = Optional.ofNullable(callback).map(callbackRef -> this.callbackSerde.serialize(recoverableProducerRecord.getRecoverableCallback())).orElse(null);

        int bufferSize = 1 // version byte size
                + SIZE_OF_INT + recoverableProducerRecord.getKey().length
                + SIZE_OF_INT + recoverableProducerRecord.getTopic().getBytes(StandardCharsets.UTF_8).length
                + SIZE_OF_INT// for partition
                + Optional.ofNullable(recordCallbackBytes).map(bytes -> (SIZE_OF_INT + bytes.length)).orElse(SIZE_OF_INT)
                + SIZE_OF_INT + recoverableProducerRecord.getMessage().length;

        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.put(RecoveryConstants.CURRENT_VERSION);
        byteBuffer.put(recoverableProducerRecord.getKey());
        ByteBufferUtils.putUTF8String(byteBuffer, recoverableProducerRecord.getTopic());
        byteBuffer.putInt(Optional.ofNullable(recoverableProducerRecord.getPartition()).orElse(-1));
        updateRecordCallback(byteBuffer, recordCallbackBytes, recoverableProducerRecord.getRecoverableCallback());
        ByteBufferUtils.putBytes(byteBuffer, recoverableProducerRecord.getMessage());
        return byteBuffer.array();
    }

    public RecoverableProducerRecord deserialize(byte[] bytes) throws RecoveryException, IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        byte[] key = ByteBufferUtils.getBytes(byteBuffer);
        String topic = ByteBufferUtils.getUTF8String(byteBuffer);
        int partition = byteBuffer.getInt();
        RecoverableCallback hevoRecordCallback = toRecoverableCallback(byteBuffer);
        byte[] message = ByteBufferUtils.getBytes(byteBuffer);
        return RecoverableProducerRecord.builder()
                .key(key).topic(topic).partition(partition == -1 ? null : partition)
                .recoverableCallback(hevoRecordCallback).message(message).build();

    }

    private void updateRecordCallback(ByteBuffer byteBuffer, byte[] bytes, RecoverableCallback recoverableCallback) {
        if (bytes != null) {
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            return;
        }
        byteBuffer.putInt(0);
    }

    private RecoverableCallback toRecoverableCallback(ByteBuffer byteBuffer) throws RecoveryException {

        int length = byteBuffer.getInt();
        if (length == 0) {
            return null;
        }
        byte[] dstArray = new byte[length];
        byteBuffer.get(dstArray);
        return this.callbackSerde.deserialize(dstArray);
    }
}
