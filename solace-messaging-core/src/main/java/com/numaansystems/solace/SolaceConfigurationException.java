package com.numaansystems.solace;

/** The library was configured with invalid or unsupported settings. */
public class SolaceConfigurationException extends SolaceMessagingException {

    private static final long serialVersionUID = 1L;

    public SolaceConfigurationException(String message) {
        super(message);
    }

    public SolaceConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
