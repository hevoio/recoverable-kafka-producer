package com.hevodata.exceptions;

public class RecoveryDisabledException extends RecoveryException {

    public RecoveryDisabledException() {
        super("Kafka recovery disabled");
    }
}
