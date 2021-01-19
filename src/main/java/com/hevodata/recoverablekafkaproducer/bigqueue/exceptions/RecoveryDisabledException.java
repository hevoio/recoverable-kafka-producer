package com.hevodata.recoverablekafkaproducer.bigqueue.exceptions;

public class RecoveryDisabledException extends RecoveryException {

    public RecoveryDisabledException() {
        super("Kafka recovery disabled");
    }
}
