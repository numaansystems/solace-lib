package com.numaansystems.solace.internal;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.RetryPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerSettingsTest {

    private static ConsumerSettings withDeadLetterQueue(String deadLetterQueue) {
        return new ConsumerSettings(1, AcknowledgeMode.AUTO, RetryPolicy.none(), Duration.ofSeconds(1),
                Duration.ofSeconds(5), deadLetterQueue);
    }

    @Test
    void noDeadLetterQueueUnlessConfiguredOrSetOnTheConsumer() {
        ConsumerSettings settings = ConsumerSettings.defaults();

        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").build())).isNull();
        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").deadLetterQueue("x").build()))
                .isEqualTo("x");
        assertThat(withDeadLetterQueue(" ").deadLetterQueueFor(ConsumerOptions.queue("orders").build())).isNull();
    }

    @Test
    void configuredDefaultAppliesToEveryConsumer() {
        ConsumerSettings settings = withDeadLetterQueue("shared.dlq");

        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").build())).isEqualTo("shared.dlq");
        assertThat(settings.deadLetterQueueFor(ConsumerOptions.topic("prices/>").build())).isEqualTo("shared.dlq");
    }

    @Test
    void placeholderIsReplacedByQueueOrDurableSubscriptionName() {
        ConsumerSettings settings = withDeadLetterQueue("{destination}.dlq");

        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").build())).isEqualTo("orders.dlq");
        assertThat(settings.deadLetterQueueFor(ConsumerOptions.durableTopic("orders/>", "billing").build()))
                .isEqualTo("billing.dlq");
        assertThat(settings.deadLetterQueueFor(ConsumerOptions.topic("prices/>").build())).isNull();
    }

    @Test
    void consumerSettingsOverrideTheConfiguredDefault() {
        ConsumerSettings settings = withDeadLetterQueue("shared.dlq");

        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").deadLetterQueue("own.dlq").build()))
                .isEqualTo("own.dlq");
        assertThat(settings.deadLetterQueueFor(ConsumerOptions.queue("orders").noDeadLetterQueue().build()))
                .isNull();
    }

    @Test
    void consumerOfTheDeadLetterQueueDoesNotForwardToItself() {
        assertThat(withDeadLetterQueue("shared.dlq")
                .deadLetterQueueFor(ConsumerOptions.queue("shared.dlq").build())).isNull();
    }
}
