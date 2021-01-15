package com.hevodata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.hevodata.exceptions.RecoveryException;
import com.hevodata.exceptions.RecoveryRuntimeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.file.Paths;
import java.util.Map;


public class RecoverableKafkaProducerSample {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    private static class DummyCallback implements RecoverableCallback {
        private String field;

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

    public void publishMessage() throws RecoveryException {

        KafkaProducer<byte[], byte[]> kafkaProducer = buildProducer();
        ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("/Users/arun/Downloads/kafka_test/")).build();
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(),
                "value".getBytes());
        recoverableKafkaProducer.publish(producerRecord);
        recoverableKafkaProducer.close();
    }

    public void publishMessageWithCallback() throws RecoveryException {
        KafkaProducer<byte[], byte[]> kafkaProducer = buildProducer();
        ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("/Users/arun/Downloads/kafka_test/"))
                .callbackSerde(new DummyCallbackSerde()).build();
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(),"value".getBytes());
        recoverableKafkaProducer.publish(producerRecord);
        recoverableKafkaProducer.close();

    }

    private KafkaProducer<byte[], byte[]> buildProducer() {
        Map<String, Object> producerProps = Maps.newHashMap();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return new KafkaProducer<>(producerProps);
    }


    public static void main(String[] args) throws Exception {
        RecoverableKafkaProducerSample kafkaProducerSample = new RecoverableKafkaProducerSample();
        kafkaProducerSample.publishMessageWithCallback();
    }
}