package com.numaansystems.solace.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceSerializationException;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Default serializer: {@code String} as text, {@code byte[]} as bytes, anything
 * else as JSON text.
 */
public class JacksonPayloadSerializer implements PayloadSerializer {

    public static final String TEXT_PLAIN = "text/plain";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

    private final ObjectMapper objectMapper;

    public JacksonPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    @Override
    public SerializedPayload serialize(Object payload, String contentType) {
        Objects.requireNonNull(payload, "payload");
        if (payload instanceof String text) {
            return SerializedPayload.text(text, contentType != null ? contentType : TEXT_PLAIN);
        }
        if (payload instanceof byte[] bytes) {
            return SerializedPayload.bytes(bytes, contentType != null ? contentType : APPLICATION_OCTET_STREAM);
        }
        try {
            return SerializedPayload.text(objectMapper.writeValueAsString(payload),
                    contentType != null ? contentType : APPLICATION_JSON);
        } catch (Exception ex) {
            throw new SolaceSerializationException(
                    "Could not serialize payload of type " + payload.getClass().getName() + " to JSON", ex);
        }
    }

    @Override
    public <T> T deserialize(SerializedPayload payload, Class<T> targetType) {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(targetType, "targetType");
        try {
            if (targetType == Object.class) {
                return targetType.cast(payload.isText() ? payload.getText() : payload.getBytes());
            }
            if (targetType == String.class) {
                return targetType.cast(payload.isText()
                        ? payload.getText()
                        : new String(payload.getBytes(), StandardCharsets.UTF_8));
            }
            if (targetType == byte[].class) {
                return targetType.cast(payload.isText()
                        ? payload.getText().getBytes(StandardCharsets.UTF_8)
                        : payload.getBytes());
            }
            return payload.isText()
                    ? objectMapper.readValue(payload.getText(), targetType)
                    : objectMapper.readValue(payload.getBytes(), targetType);
        } catch (Exception ex) {
            throw new SolaceSerializationException(
                    "Could not deserialize payload to " + targetType.getName(), ex);
        }
    }
}
