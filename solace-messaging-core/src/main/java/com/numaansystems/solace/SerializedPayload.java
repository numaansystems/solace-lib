package com.numaansystems.solace;

import java.util.Objects;

/**
 * Wire form of a payload: either text (sent as a JMS {@code TextMessage}) or
 * bytes (sent as a JMS {@code BytesMessage}), plus its content type.
 */
public final class SerializedPayload {

    private final String text;
    private final byte[] bytes;
    private final String contentType;

    private SerializedPayload(String text, byte[] bytes, String contentType) {
        this.text = text;
        this.bytes = bytes;
        this.contentType = contentType;
    }

    public static SerializedPayload text(String text, String contentType) {
        return new SerializedPayload(Objects.requireNonNull(text, "text"), null, contentType);
    }

    public static SerializedPayload bytes(byte[] bytes, String contentType) {
        return new SerializedPayload(null, Objects.requireNonNull(bytes, "bytes").clone(), contentType);
    }

    public boolean isText() {
        return text != null;
    }

    /** The text body, or {@code null} for a bytes payload. */
    public String getText() {
        return text;
    }

    /** A copy of the byte body, or {@code null} for a text payload. */
    public byte[] getBytes() {
        return bytes == null ? null : bytes.clone();
    }

    /** May be {@code null} when the sender did not set one. */
    public String getContentType() {
        return contentType;
    }
}
