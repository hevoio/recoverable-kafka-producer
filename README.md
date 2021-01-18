# Recoverable Kafka Producer

When you write to a kafka broker using the producer library, the records are first written to a kafka in-memory buffer and the kafka [*sender*](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java) thread is responsible for reading the records from the buffer and reliably syncing those to the broker. The client application can choose to wait for the records to be synced to the broker and only then perform the next steps in the workflow. This mode is very expensive and will not be acceptable for applications which require sub millisecond latencies. Those workflows might choose to just publish the record to the buffer and continue with the rest of the workflow. In such cases, its possible that the records in the buffer might get dropped due to various reasons(application crash, kafka broker crash etc). *Recoverable Producer* was build to solve this problem of achieving data integrity at scale, at Hevo.

*Recovery Producer* works by writing the records to a local, memory-mapped write ahead log before writing to the kafka buffer and having periodic check-pointing of record offsets for which we have got success/failure callbacks. Recoverable producer uses [*Big Queue*](https://github.com/bulldog2011/bigqueue), which provides memory-mapped queues/arrays out of the box and also provides submillisecond latencies. In case of non graceful shutdowns of the recoverable producer, producer will recover possible lost records by replaying from the latest committed check-point. The records, which the sender thread is not able to sync it to the broker will also be pushed to a BigQueue and retried periodically.

## Maven Dependency
```
  <dependency>
    <groupId>com.hevodata</groupId>
    <artifactId>recoverable-kafka-producer</artifactId>
    <version>1.0</version>
  </dependency>
```    

## Delivery Semantics

*Recoverable Producer* gives atleast-once semantics and its also possible that some of the records are delivered out of order(in case of failure callbacks).

## Configurations

```
  KafkaProducer<byte[], byte[]> kafkaProducer = buildProducer();
  ProducerRecoveryConfig producerRecoveryConfig = ProducerRecoveryConfig.builder().baseDir(Paths.get("kafka_test"))
    .recordTrackerConfig(new RecordTrackerConfig(5)).callbackSerde(new DummyCallbackSerde()).maxParallelism(10).build();
    
  RecoverableKafkaProducer recoverableKafkaProducer = new RecoverableKafkaProducer(kafkaProducer, producerRecoveryConfig);
  ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>("topic1", null, "key".getBytes(), "value".getBytes());
  
  recoverableKafkaProducer.publish(producerRecord, new DummyCallback("field_value"));
```

More sample usages of the recoverable producer can be found [here](https://github.com/hevoio/recoverable-kafka-producer/blob/master/src/main/java/com/hevodata/samples/SampleRecoverableKafkaProducer.java). There are few configurations that need to be kept in mind while using the recoverable producer.

### Max parallelism

This parameter indicates the max number of parallel threads, which can perform a publish on the same producer simultaneously. This value is used to work around a multi-threaded edge case around the recoverable producer. Default value is 100.

### Flush frequency

This controls the frequency(in seconds), in which offset check-pointing will be performed. Please note that check-pointing consists of publishing the latest committed offset to a local file and also removing the records before the committed offset, from the disk. Default value is 5 seconds.

### Disk Threshold

This parameter puts an upper bound on the local disk space, which the producer can occupy to store the records till the callback is received and flush is performed. This needs to be configured based on the configured kafka buffer size, flush frequency and also the write throughput. In case of disk threshold breach, further attempts to publish the record will result in [RecoveryDisabledException](https://github.com/hevoio/recoverable-kafka-producer/blob/master/src/main/java/com/hevodata/exceptions/RecoveryDisabledException.java). Default value is 20 GB.

## Serializing/Deserializing Callbacks

In case a [RecoverableCallback](https://github.com/hevoio/recoverable-kafka-producer/blob/master/src/main/java/com/hevodata/RecoverableCallback.java) is used with the recoverable producer, a CallbackSerde should  be provided in the producer configuration to serialize/deserialize callbacks. Please note that the same producer cannot be used with different callback classes. In such cases, we recommend using different producers or handling it upstream by encapsulating the logic into a single RecoverableCallback class.


## Performance

The recoverable producer ideally just adds a few microseconds in addition to the latency added by the kafka producer. But it can vary based on a lot of factors like message size, environment specs etc. Some of the benchmarks done by BigQueue can be found [here](https://github.com/bulldog2011/bigqueue/wiki/Performance-Test-Report).

## Logging

Recoverable producer uses *slf4j* as the logging facade. A slf4j compatible logging framework needs to be bound to enable logging on the producer side.

Please write to dev@hevodata.com for any queries/feedback.
    
