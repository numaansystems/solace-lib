package com.numaansystems.solace;

/** The broker connection could not be established or was lost. */
public class SolaceConnectionException extends SolaceMessagingException {

    private static final long serialVersionUID = 1L;

    public SolaceConnectionException(String message) {
        super(message);
    }

    public SolaceConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
