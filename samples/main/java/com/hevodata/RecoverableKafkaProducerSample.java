package com.hevodata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.hevodata.exceptions.RecoveryException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.file.Paths;
import java.util.Map;


public class RecoverableKafkaProducerSample {

    public void runSimpleProducer() throws RecoveryException {

        Map<String, Object> producerProps = Maps.newHashMap();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<>(producerProps);
        ProducerRecoveryConfig producerRecoveryConfig = new ProducerRecoveryConfig(Paths.get("/Users/arun/Downloads/kafka_test/"), 100, 20, null);
        RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(),
                "value".getBytes());
        recoverableKafkaProducer.publish(producerRecord);
        recoverableKafkaProducer.close();
    }

    public static void main(String[] args) throws Exception {
        RecoverableKafkaProducerSample kafkaProducerSample = new RecoverableKafkaProducerSample();
        kafkaProducerSample.runSimpleProducer();
    }
}
