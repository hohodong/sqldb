package com.database.concurrency;

@SuppressWarnings("serial")
public class DuplicateLockRequestException extends RuntimeException {
    DuplicateLockRequestException(String message) {
        super(message);
    }
}

