package com.numaansystems.solace;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OptionsTest {

    @Test
    void publishOptionsDefaultToTopicWithNoOverrides() {
        PublishOptions options = PublishOptions.defaults();

        assertThat(options.getDestinationType()).isEqualTo(DestinationType.TOPIC);
        assertThat(options.getDeliveryMode()).isNull();
        assertThat(options.getProperties()).isEmpty();
    }

    @Test
    void publishOptionsCarryRequestReplyMetadata() {
        PublishOptions options = PublishOptions.builder()
                .deliveryMode(DeliveryMode.PERSISTENT)
                .correlationId("REQ-1")
                .replyTo("pricing/response/clientA")
                .property("tenant", "acme")
                .property("attempt", 2)
                .build();

        assertThat(options.getCorrelationId()).isEqualTo("REQ-1");
        assertThat(options.getReplyTo()).isEqualTo("pricing/response/clientA");
        assertThat(options.getReplyToType()).isEqualTo(DestinationType.TOPIC);
        assertThat(options.getProperties()).containsEntry("tenant", "acme").containsEntry("attempt", 2);
    }

    @Test
    void publishOptionsPropertiesAreImmutable() {
        PublishOptions options = PublishOptions.builder().property("a", "b").build();

        assertThatThrownBy(() -> options.getProperties().put("c", "d"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void propertyNamesMustFollowJmsRules() {
        PublishOptions.Builder builder = PublishOptions.builder();

        assertThatThrownBy(() -> builder.property("has-dash", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property("1leadingDigit", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property("JMSXGroupID", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property("like", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property("", "x")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property(SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY, "x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void propertyValuesMustBeJmsPrimitivesOrStrings() {
        PublishOptions.Builder builder = PublishOptions.builder();

        assertThatThrownBy(() -> builder.property("when", java.time.Instant.now()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> builder.property("nothing", null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void priorityAndTimeoutsAreRangeChecked() {
        assertThatThrownBy(() -> PublishOptions.builder().priority(10)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PublishOptions.builder().timeToLive(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PublishOptions.builder().sendTimeout(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void queueConsumerHasNoSubscriptionType() {
        ConsumerOptions options = ConsumerOptions.queue("orders").concurrency(4).maxConcurrency(8).build();

        assertThat(options.getDestinationType()).isEqualTo(DestinationType.QUEUE);
        assertThat(options.getSubscriptionType()).isNull();
        assertThat(options.getConcurrency()).isEqualTo(4);
    }

    @Test
    void durableTopicNeedsSubscriptionName() {
        assertThat(ConsumerOptions.durableTopic("orders/>", "billing").build().getSubscriptionType())
                .isEqualTo(SubscriptionType.DURABLE);
        assertThatThrownBy(() -> ConsumerOptions.durableTopic("orders/>", " ").build())
                .isInstanceOf(SolaceConfigurationException.class);
    }

    @Test
    void topicSubscriptionsRejectConcurrencyAboveOne() {
        assertThatThrownBy(() -> ConsumerOptions.topic("prices/>").concurrency(2).build())
                .isInstanceOf(SolaceConfigurationException.class)
                .hasMessageContaining("concurrency 1");
        assertThatThrownBy(() -> ConsumerOptions.durableTopic("prices/>", "sub").maxConcurrency(3).build())
                .isInstanceOf(SolaceConfigurationException.class);
    }

    @Test
    void invalidConsumerSettingsAreRejected() {
        assertThatThrownBy(() -> ConsumerOptions.queue(" ").build()).isInstanceOf(SolaceConfigurationException.class);
        assertThatThrownBy(() -> ConsumerOptions.queue("q").concurrency(0).build())
                .isInstanceOf(SolaceConfigurationException.class);
        assertThatThrownBy(() -> ConsumerOptions.queue("q").concurrency(4).maxConcurrency(2).build())
                .isInstanceOf(SolaceConfigurationException.class);
    }

    @Test
    void retryPolicyBacksOffExponentiallyUpToTheCap() {
        RetryPolicy policy = new RetryPolicy(5, Duration.ofMillis(100), 2.0, Duration.ofMillis(350));

        assertThat(policy.delayAfterAttempt(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(policy.delayAfterAttempt(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(policy.delayAfterAttempt(3)).isEqualTo(Duration.ofMillis(350));
        assertThat(RetryPolicy.none().maxAttempts()).isEqualTo(1);
        assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, 1.0, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void solaceMessageKeepsMetadataWhenPayloadChanges() {
        SolaceMessage<String> original = SolaceMessage.<String>builder()
                .payload("{}")
                .providerMessageId("ID:1")
                .correlationId("REQ-1")
                .replyTo("reply/topic", DestinationType.TOPIC)
                .property("tenant", "acme")
                .build();

        SolaceMessage<Integer> converted = original.withPayload(42);

        assertThat(converted.getPayload()).isEqualTo(42);
        assertThat(converted.getCorrelationId()).isEqualTo("REQ-1");
        assertThat(converted.getReplyTo()).isEqualTo("reply/topic");
        assertThat(converted.getMessageId()).isEqualTo("ID:1");
        assertThat(converted.getProperties()).containsEntry("tenant", "acme");
        assertThat(converted.toString()).doesNotContain("42");
    }
}
