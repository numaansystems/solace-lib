package com.numaansystems.solace;

/** A message could not be consumed, acknowledged or handed to its handler. */
public class SolaceConsumeException extends SolaceMessagingException {

    private static final long serialVersionUID = 1L;

    public SolaceConsumeException(String message) {
        super(message);
    }

    public SolaceConsumeException(String message, Throwable cause) {
        super(message, cause);
    }
}
