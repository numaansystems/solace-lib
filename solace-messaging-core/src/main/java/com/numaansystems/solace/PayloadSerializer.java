package com.numaansystems.solace;

/**
 * Converts application payloads to and from their wire form. Applications can
 * replace the default (Jackson-based) implementation by declaring their own
 * bean of this type.
 */
public interface PayloadSerializer {

    /**
     * @param payload     the application payload, never {@code null}
     * @param contentType content type requested by the caller, or {@code null}
     *                    to let the serializer choose
     * @throws SolaceSerializationException if the payload cannot be converted
     */
    SerializedPayload serialize(Object payload, String contentType);

    /**
     * @param targetType the type requested by the consumer; {@code Object.class}
     *                   means "as received" ({@code String} or {@code byte[]})
     * @throws SolaceSerializationException if the payload cannot be converted
     */
    <T> T deserialize(SerializedPayload payload, Class<T> targetType);
}
