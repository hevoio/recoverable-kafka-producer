package com.hevodata.recoverablekafkaproducer.bigqueue.exceptions;

public class RecoveryException extends Exception {

    public RecoveryException(String message, Throwable cause) {
        super(message, cause);
    }

    public RecoveryException(String message) {
        super(message);
    }

    public RecoveryException(Throwable cause) {
        super(cause);
    }
}
