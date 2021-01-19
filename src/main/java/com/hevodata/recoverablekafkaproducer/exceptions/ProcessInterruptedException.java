package com.hevodata.recoverablekafkaproducer.exceptions;

public class ProcessInterruptedException extends RecoveryException {

    public ProcessInterruptedException(String message) {
        super(message);
    }
}
