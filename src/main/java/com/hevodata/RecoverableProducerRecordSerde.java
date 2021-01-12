package com.hevodata;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


@SuppressWarnings("unchecked")
public class RecoverableProducerRecordSerde {

    public RecoverableProducerRecordSerde(Set<RecoverableRecordCallbackHelper> recoverableRecordCallbackHelpers) {
        this.recoverableRecordCallbackHelperMap = recoverableRecordCallbackHelpers.stream().collect
                (Collectors.toMap(RecoverableRecordCallbackHelper::id, Function.identity()));

    }

    public byte[] serialize(RecoverableProducerRecord recoverableProducerRecord) throws HevoException {

        HevoRecordCallback hevoRecordCallback = recoverableProducerRecord.getHevoRecordCallback();
        byte[] recordCallbackBytes = toBytes(hevoRecordCallback);
        byte[] recordTagBytes = toBytes(recoverableProducerRecord.getTags());

        int bufferSize = 1 // version byte size
                + 1 // backup byte size
                + SIZE_OF_INT + recoverableProducerRecord.getKey().getBytes(StandardCharsets.UTF_8).length
                + SIZE_OF_INT + recoverableProducerRecord.getTopic().getBytes(StandardCharsets.UTF_8).length
                + SIZE_OF_INT// for partition
                + Optional.ofNullable(recordCallbackBytes).map(bytes -> (SIZE_OF_INT + bytes.length + SIZE_OF_SHORT)).orElse(SIZE_OF_INT)
                + Optional.ofNullable(recordTagBytes).map(bytes -> (SIZE_OF_INT + bytes.length)).orElse(SIZE_OF_INT)
                + SIZE_OF_INT + recoverableProducerRecord.getMessage().length;

        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.put(CURRENT_VERSION);
        byteBuffer.put(recoverableProducerRecord.isBackup() ? (byte) 1 : 0);
        ByteBufferUtils.putUTF8String(byteBuffer, recoverableProducerRecord.getKey());
        ByteBufferUtils.putUTF8String(byteBuffer, recoverableProducerRecord.getTopic());
        byteBuffer.putInt(Optional.ofNullable(recoverableProducerRecord.getPartition()).orElse(-1));
        updateRecordCallback(byteBuffer, recordCallbackBytes, recoverableProducerRecord.getHevoRecordCallback());
        ByteBufferUtils.putBytes(byteBuffer, recordTagBytes);
        ByteBufferUtils.putBytes(byteBuffer, recoverableProducerRecord.getMessage());
        return byteBuffer.array();
    }

    public RecoverableProducerRecord deserialize(byte[] bytes) throws HevoException, IOException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byte version = byteBuffer.get();
        boolean backup = false;
        if (version != 1) {
            backup = byteBuffer.get() != 0;
        }

        String key = ByteBufferUtils.getUTF8String(byteBuffer);
        String topic = ByteBufferUtils.getUTF8String(byteBuffer);
        int partition = byteBuffer.getInt();
        HevoRecordCallback hevoRecordCallback = toRecordCallback(byteBuffer);
        List<HevoRecordTag> hevoRecordTags = toRecordTags(ByteBufferUtils.getBytes(byteBuffer));
        byte[] message = ByteBufferUtils.getBytes(byteBuffer);
        return RecoverableProducerRecord.builder()
                .key(key).topic(topic).partition(partition == -1 ? null : partition)
                .hevoRecordCallback(hevoRecordCallback).tags(hevoRecordTags).message(message).backup(backup).build();

    }

    private void updateRecordCallback(ByteBuffer byteBuffer, byte[] bytes, HevoRecordCallback hevoRecordCallback) {
        if (bytes != null) {
            RecoverableRecordCallback recoverableRecordCallback = (RecoverableRecordCallback) hevoRecordCallback;
            byteBuffer.putShort(recoverableRecordCallback.callbackHelper().id());
            byteBuffer.putInt(bytes.length);
            byteBuffer.put(bytes);
            return;
        }
        byteBuffer.putShort((short)-1);

    }

    private HevoRecordCallback toRecordCallback(ByteBuffer byteBuffer) throws HevoException {

        short callBackId = byteBuffer.getShort();
        if (callBackId == -1) {
            return null;
        }
        int length = byteBuffer.getInt();
        byte[] dstArray = new byte[length];
        byteBuffer.get(dstArray);
        return this.recoverableRecordCallbackHelperMap.get(callBackId).fromBytes(dstArray);
    }


    private byte[] toBytes(HevoRecordCallback hevoRecordCallback) throws HevoException {
        if (hevoRecordCallback instanceof RecoverableRecordCallback) {
            RecoverableRecordCallback recoverableRecordCallback = (RecoverableRecordCallback) hevoRecordCallback;
            return this.recoverableRecordCallbackHelperMap.get(recoverableRecordCallback.callbackHelper().id())
                    .toBytes(recoverableRecordCallback);

        }
        return null;
    }

    private byte[] toBytes(List<HevoRecordTag> recordTags) throws HevoException {
        if (CollectionUtils.isNotEmpty(recordTags)) {
            return JsonUtils.objectToByteArray(recordTags);
        }
        return null;
    }

    private List<HevoRecordTag> toRecordTags(byte[] bytes) throws IOException {
        if (bytes == null) {
            return Lists.newArrayList();
        }
        return JsonUtils.byteArrayToTypeReference(bytes, new TypeReference<List<HevoRecordTag>>() {
        });

    }

}
