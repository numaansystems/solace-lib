package com.numaansystems.solace;

/** A payload could not be converted to or from its wire form. */
public class SolaceSerializationException extends SolaceMessagingException {

    private static final long serialVersionUID = 1L;

    public SolaceSerializationException(String message) {
        super(message);
    }

    public SolaceSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
