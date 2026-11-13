package com.numaansystems.solace;

/** A message could not be published, or its publish outcome is not known. */
public class SolacePublishException extends SolaceMessagingException {

    private static final long serialVersionUID = 1L;

    private final boolean outcomeUnknown;

    public SolacePublishException(String message, Throwable cause, boolean outcomeUnknown) {
        super(message, cause);
        this.outcomeUnknown = outcomeUnknown;
    }

    /**
     * {@code true} when the failure happened after the message was handed to
     * the broker client (or the send timed out), so the broker may or may not
     * have accepted it. Publishing again can then produce a duplicate.
     * {@code false} when the message was definitely not sent.
     */
    public boolean isOutcomeUnknown() {
        return outcomeUnknown;
    }
}
