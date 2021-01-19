package com.hevodata.recoverablekafkaproducer.exceptions;

public class RecoveryDisabledException extends RecoveryException {

    public RecoveryDisabledException() {
        super("Kafka recovery disabled");
    }
}
