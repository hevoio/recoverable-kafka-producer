package com.hevodata.recoverablekafkaproducer.bigqueue.samples;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.hevodata.recoverablekafkaproducer.bigqueue.CallbackSerde;
import com.hevodata.recoverablekafkaproducer.bigqueue.RecoverableCallback;
import com.hevodata.recoverablekafkaproducer.bigqueue.RecoverableKafkaProducer;
import com.hevodata.recoverablekafkaproducer.bigqueue.commons.Utils;
import com.hevodata.recoverablekafkaproducer.bigqueue.config.ProducerRecoveryConfig;
import com.hevodata.recoverablekafkaproducer.bigqueue.config.RecordTrackerConfig;
import com.hevodata.recoverablekafkaproducer.bigqueue.exceptions.RecoveryException;
import com.hevodata.recoverablekafkaproducer.bigqueue.exceptions.RecoveryRuntimeException;
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
import java.util.concurrent.atomic.AtomicLong;


public class SampleRecoverableKafkaProducer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    private static class DummyCallback implements RecoverableCallback {
        private String field;
        private static final AtomicLong seqNo = new AtomicLong(0);

        @Override
        public void onSuccess(RecordMetadata metadata) {
            System.out.println("Inside onSuccess:" + seqNo.incrementAndGet());
        }

        @Override
        public void onFailure(RecordMetadata metadata, Exception e, boolean recovered) {
            System.out.println("Inside onFailure: " + seqNo.incrementAndGet() + " and field " + field);
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
        ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("kafka_test")).build();
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(),
                "value".getBytes());
        recoverableKafkaProducer.publish(producerRecord);
        recoverableKafkaProducer.close();
    }

    public void publishMessageWithCallback() throws RecoveryException {
        KafkaProducer<byte[], byte[]> kafkaProducer = buildProducer();
        ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("kafka_test"))
                .recordTrackerConfig(new RecordTrackerConfig(5))
                .callbackSerde(new DummyCallbackSerde()).maxParallelism(10).build();
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        for (int i = 0; i < 10; i++) {
            ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(), "value".getBytes());
            recoverableKafkaProducer.publish(producerRecord, new DummyCallback("" + i));
        }
        Utils.interruptIgnoredSleep(100000);

    }

    public void cleanup() throws RecoveryException {
        KafkaProducer<byte[], byte[]> kafkaProducer = buildProducer();
        ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("kafka_test"))
                .recordTrackerConfig(new RecordTrackerConfig(1))
                .callbackSerde(new DummyCallbackSerde()).maxParallelism(10).build();
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        recoverableKafkaProducer.cleanUp();

    }

    private KafkaProducer<byte[], byte[]> buildProducer() {
        Map<String, Object> producerProps = Maps.newHashMap();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return new KafkaProducer<>(producerProps);
    }


    public static void main(String[] args) throws Exception {
        SampleRecoverableKafkaProducer sampleRecoverableKafkaProducer = new SampleRecoverableKafkaProducer();
        sampleRecoverableKafkaProducer.publishMessage();
        sampleRecoverableKafkaProducer.publishMessageWithCallback();
        sampleRecoverableKafkaProducer.cleanup();
    }
}
