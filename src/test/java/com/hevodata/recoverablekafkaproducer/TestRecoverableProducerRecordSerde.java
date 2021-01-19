package com.hevodata.recoverablekafkaproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hevodata.recoverablekafkaproducer.CallbackSerde;
import com.hevodata.recoverablekafkaproducer.RecoverableCallback;
import com.hevodata.recoverablekafkaproducer.RecoverableProducerRecord;
import com.hevodata.recoverablekafkaproducer.RecoverableProducerRecordSerde;
import com.hevodata.recoverablekafkaproducer.exceptions.RecoveryRuntimeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

public class TestRecoverableProducerRecordSerde {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    private static class DummyCallback implements RecoverableCallback {
        private int field1;
        private String field2;
        private double field3;

        @Override
        public void onSuccess(RecordMetadata metadata) {
            System.out.println("Inside onSuccess");
        }

        @Override
        public void onFailure(RecordMetadata metadata, Exception e, boolean recovered) {
            System.out.println("Inside onFailure");
        }
    }

    public static class DummyCallbackSerde implements CallbackSerde {

        @Override
        public byte[] serialize(RecoverableCallback callback) {
            try {
                return objectMapper.writeValueAsBytes(callback);
            } catch (Exception e) {
                throw new RecoveryRuntimeException(e.getMessage());
            }

        }

        @Override
        public RecoverableCallback deserialize(byte[] bytes) {
            try {
                return objectMapper.readValue(bytes, DummyCallback.class);
            } catch (Exception e) {
                throw new RecoveryRuntimeException(e.getMessage());
            }
        }
    }

    @Test
    public void testSerde() throws Exception {
        RecoverableProducerRecordSerde recoverableProducerRecordSerde = new RecoverableProducerRecordSerde(null);
        RecoverableProducerRecord recoverableProducerRecord = RecoverableProducerRecord.builder()
                .key("key".getBytes()).message("value".getBytes()).topic("topic1").build();
        byte[] bytes = recoverableProducerRecordSerde.serialize(recoverableProducerRecord);
        RecoverableProducerRecord deserializedRecord = recoverableProducerRecordSerde.deserialize(bytes);
        Assert.assertEquals(new String(deserializedRecord.getKey()), "key");
        Assert.assertEquals(new String(deserializedRecord.getMessage()), "value");
        Assert.assertEquals(deserializedRecord.getTopic(), "topic1");

        recoverableProducerRecordSerde = new RecoverableProducerRecordSerde(new DummyCallbackSerde());
        recoverableProducerRecord = RecoverableProducerRecord.builder()
                .recoverableCallback(new DummyCallback(1, "abcd", 5.678))
                .key("key".getBytes()).message("value".getBytes()).topic("topic1").build();
        bytes = recoverableProducerRecordSerde.serialize(recoverableProducerRecord);
        deserializedRecord = recoverableProducerRecordSerde.deserialize(bytes);
        Assert.assertEquals(new String(deserializedRecord.getKey()), "key");
        Assert.assertEquals(new String(deserializedRecord.getMessage()), "value");
        Assert.assertEquals(deserializedRecord.getTopic(), "topic1");
        Assert.assertNotNull(deserializedRecord.getRecoverableCallback());
        Assert.assertTrue(deserializedRecord.getRecoverableCallback() instanceof DummyCallback);
        DummyCallback dummyCallback = (DummyCallback) deserializedRecord.getRecoverableCallback();
        Assert.assertEquals(dummyCallback.getField1(), 1);
        Assert.assertEquals(dummyCallback.getField2(), "abcd");
    }

}
