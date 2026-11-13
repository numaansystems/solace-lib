package com.numaansystems.solace;

/** Base type for every exception thrown by the library. */
public class SolaceMessagingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SolaceMessagingException(String message) {
        super(message);
    }

    public SolaceMessagingException(String message, Throwable cause) {
        super(message, cause);
    }
}
