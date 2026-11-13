package com.numaansystems.solace.internal;

import tools.jackson.databind.json.JsonMapper;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceSerializationException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JacksonPayloadSerializerTest {

    public record Quote(String symbol, double price) {
    }

    public static class Unserializable {
        public String getValue() {
            throw new IllegalStateException("cannot be read");
        }
    }

    private final JacksonPayloadSerializer serializer = new JacksonPayloadSerializer(JsonMapper.builder().build());

    @Test
    void stringsAreSentAsPlainText() {
        SerializedPayload payload = serializer.serialize("hello", null);

        assertThat(payload.isText()).isTrue();
        assertThat(payload.getText()).isEqualTo("hello");
        assertThat(payload.getContentType()).isEqualTo("text/plain");
    }

    @Test
    void byteArraysAreSentAsBytes() {
        SerializedPayload payload = serializer.serialize(new byte[] {1, 2, 3}, null);

        assertThat(payload.isText()).isFalse();
        assertThat(payload.getBytes()).containsExactly(1, 2, 3);
        assertThat(payload.getContentType()).isEqualTo("application/octet-stream");
    }

    @Test
    void objectsAreSentAsJsonAndReadBack() {
        SerializedPayload payload = serializer.serialize(new Quote("EURUSD", 1.08), null);

        assertThat(payload.getContentType()).isEqualTo("application/json");
        assertThat(serializer.deserialize(payload, Quote.class)).isEqualTo(new Quote("EURUSD", 1.08));
    }

    @Test
    void callerContentTypeWins() {
        assertThat(serializer.serialize("<a/>", "application/xml").getContentType()).isEqualTo("application/xml");
    }

    @Test
    void rawTargetTypeReturnsPayloadAsReceived() {
        assertThat(serializer.deserialize(SerializedPayload.text("x", null), Object.class)).isEqualTo("x");
        assertThat(serializer.deserialize(SerializedPayload.bytes(new byte[] {7}, null), Object.class))
                .isEqualTo(new byte[] {7});
    }

    @Test
    void textAndBytesConvertBetweenEachOther() {
        byte[] utf8 = "héllo".getBytes(StandardCharsets.UTF_8);

        assertThat(serializer.deserialize(SerializedPayload.bytes(utf8, null), String.class)).isEqualTo("héllo");
        assertThat(serializer.deserialize(SerializedPayload.text("héllo", null), byte[].class)).isEqualTo(utf8);
        assertThat(serializer.deserialize(
                SerializedPayload.bytes("{\"symbol\":\"A\",\"price\":1.0}".getBytes(StandardCharsets.UTF_8), null),
                Quote.class)).isEqualTo(new Quote("A", 1.0));
    }

    @Test
    void failuresAreReportedAsSerializationExceptions() {
        assertThatThrownBy(() -> serializer.deserialize(SerializedPayload.text("not json", null), Quote.class))
                .isInstanceOf(SolaceSerializationException.class)
                .hasCauseInstanceOf(Exception.class);
        assertThatThrownBy(() -> serializer.serialize(new Unserializable(), null))
                .isInstanceOf(SolaceSerializationException.class);
    }
}
