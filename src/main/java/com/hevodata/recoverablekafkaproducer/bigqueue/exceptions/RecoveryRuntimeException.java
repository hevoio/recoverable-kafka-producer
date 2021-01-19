package com.hevodata.recoverablekafkaproducer.bigqueue.exceptions;

public class RecoveryRuntimeException extends RuntimeException {

    public RecoveryRuntimeException(String message) {
        super(message);
    }

    public RecoveryRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
