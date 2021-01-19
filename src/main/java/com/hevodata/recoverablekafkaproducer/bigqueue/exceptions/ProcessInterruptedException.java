package com.hevodata.recoverablekafkaproducer.bigqueue.exceptions;

public class ProcessInterruptedException extends RecoveryException {

    public ProcessInterruptedException(String message) {
        super(message);
    }
}
