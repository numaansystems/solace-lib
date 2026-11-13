package com.numaansystems.solace.internal;

import tools.jackson.databind.json.JsonMapper;
import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.PublishResult;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SolacePublishException;
import com.numaansystems.solace.SolaceSerializationException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DefaultSolacePublisherTest {

    private final ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
    private final Connection connection = mock(Connection.class);
    private final Session session = mock(Session.class);
    private final MessageProducer producer = mock(MessageProducer.class);
    private final TextMessage textMessage = mock(TextMessage.class);
    private final Topic topic = mock(Topic.class);
    private final Queue queue = mock(Queue.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    private DefaultSolacePublisher publisher;

    public static class Unserializable {
        public String getValue() {
            throw new IllegalStateException("cannot be read");
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);
        when(session.createTextMessage(anyString())).thenReturn(textMessage);
        when(session.createTopic(anyString())).thenReturn(topic);
        when(session.createQueue(anyString())).thenReturn(queue);
        when(session.createProducer(any())).thenReturn(producer);
        when(textMessage.getJMSMessageID()).thenReturn("ID:broker-1");
    }

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
    }

    private DefaultSolacePublisher publisher(RetryPolicy retry, boolean retryOnUnknownOutcome) {
        publisher = new DefaultSolacePublisher(connectionFactory,
                new JacksonPayloadSerializer(JsonMapper.builder().build()),
                new PublisherSettings(DeliveryMode.PERSISTENT, Duration.ofSeconds(5), true, retry,
                        retryOnUnknownOutcome, 2, 10),
                new SolaceMetrics(meterRegistry));
        return publisher;
    }

    private static RetryPolicy retries(int maxAttempts) {
        return new RetryPolicy(maxAttempts, Duration.ZERO, 1.0, Duration.ZERO);
    }

    @Test
    void publishesPersistentToTopicWithCorrelationAndReplyTo() throws Exception {
        PublishResult result = publisher(RetryPolicy.none(), false).publish("pricing/request", "{}",
                PublishOptions.builder()
                        .correlationId("REQ-123456")
                        .replyTo("pricing/response/clientA")
                        .build());

        verify(producer).send(topic, textMessage, jakarta.jms.DeliveryMode.PERSISTENT, 4, 0L);
        verify(textMessage).setJMSCorrelationID("REQ-123456");
        verify(textMessage).setJMSReplyTo(topic);
        assertThat(result.destination()).isEqualTo("pricing/request");
        assertThat(result.deliveryMode()).isEqualTo(DeliveryMode.PERSISTENT);
        assertThat(result.providerMessageId()).isEqualTo("ID:broker-1");
        assertThat(result.messageId()).isNotBlank();
        assertThat(result.attempts()).isEqualTo(1);
        assertThat(meterRegistry.get("solace.publish").tag("outcome", "success").timer().count()).isEqualTo(1);
    }

    @Test
    void publishesNonPersistentToQueueWithTtlPriorityAndMessageId() throws Exception {
        PublishResult result = publisher(RetryPolicy.none(), false).publish("orders.inbound", "x",
                PublishOptions.builder()
                        .destinationType(DestinationType.QUEUE)
                        .deliveryMode(DeliveryMode.NON_PERSISTENT)
                        .timeToLive(Duration.ofSeconds(30))
                        .priority(8)
                        .messageId("order-42")
                        .build());

        verify(producer).send(queue, textMessage, jakarta.jms.DeliveryMode.NON_PERSISTENT, 8, 30_000L);
        assertThat(result.messageId()).isEqualTo("order-42");
        assertThat(result.destinationType()).isEqualTo(DestinationType.QUEUE);
    }

    @Test
    void failureBeforeSendIsRetriedBecauseNothingWasSent() throws Exception {
        when(connectionFactory.createConnection())
                .thenThrow(new JMSException("broker unreachable"))
                .thenReturn(connection);

        PublishResult result = publisher(retries(2), false).publish("t", "x");

        assertThat(result.attempts()).isEqualTo(2);
        verify(producer, times(1)).send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
        assertThat(meterRegistry.get("solace.publish.retries").counter().count()).isEqualTo(1);
    }

    @Test
    void failureDuringSendIsNotRetriedByDefaultAndReportsUnknownOutcome() throws Exception {
        doThrow(new JMSException("connection lost")).when(producer)
                .send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
        DefaultSolacePublisher retrying = publisher(retries(3), false);

        SolacePublishException failure =
                catchThrowableOfType(SolacePublishException.class, () -> retrying.publish("t", "x"));

        assertThat(failure.isOutcomeUnknown()).isTrue();
        assertThat(failure).hasRootCauseInstanceOf(JMSException.class);
        verify(producer, times(1)).send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
    }

    @Test
    void failureDuringSendIsRetriedWhenExplicitlyAllowed() throws Exception {
        doThrow(new JMSException("connection lost")).when(producer)
                .send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
        DefaultSolacePublisher retrying = publisher(retries(3), true);

        assertThatThrownBy(() -> retrying.publish("t", "x")).isInstanceOf(SolacePublishException.class);

        verify(producer, times(3)).send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
    }

    @Test
    void connectionFailureWithoutRetryReportsMessageNotSent() throws Exception {
        when(connectionFactory.createConnection()).thenThrow(new JMSException("broker unreachable"));
        DefaultSolacePublisher noRetry = publisher(RetryPolicy.none(), false);

        SolacePublishException failure =
                catchThrowableOfType(SolacePublishException.class, () -> noRetry.publish("t", "x"));

        assertThat(failure.isOutcomeUnknown()).isFalse();
        verify(producer, never()).send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
    }

    @Test
    void sendTimeoutReportsUnknownOutcome() throws Exception {
        doAnswer(invocation -> {
            Thread.sleep(1_000);
            return null;
        }).when(producer).send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
        DefaultSolacePublisher slow = publisher(RetryPolicy.none(), false);

        SolacePublishException failure = catchThrowableOfType(SolacePublishException.class,
                () -> slow.publish("t", "x", PublishOptions.builder().sendTimeout(Duration.ofMillis(100)).build()));

        assertThat(failure.isOutcomeUnknown()).isTrue();
    }

    @Test
    void serializationFailureIsReportedBeforeAnythingIsSent() throws Exception {
        DefaultSolacePublisher noRetry = publisher(RetryPolicy.none(), false);

        assertThatThrownBy(() -> noRetry.publish("t", new Unserializable()))
                .isInstanceOf(SolaceSerializationException.class);
        verify(connectionFactory, never()).createConnection();
    }

    @Test
    void asyncPublishCompletesWithResultOrFailure() throws Exception {
        DefaultSolacePublisher async = publisher(RetryPolicy.none(), false);

        assertThat(async.publishAsync("t", "x", PublishOptions.defaults()).get().attempts()).isEqualTo(1);

        doThrow(new JMSException("connection lost")).when(producer)
                .send(any(Destination.class), any(Message.class), anyInt(), anyInt(), anyLong());
        assertThatThrownBy(() -> async.publishAsync("t", "x", PublishOptions.defaults()).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(SolacePublishException.class);
    }

    @Test
    void blankDestinationIsRejected() {
        DefaultSolacePublisher noRetry = publisher(RetryPolicy.none(), false);

        assertThatThrownBy(() -> noRetry.publish(" ", "x")).isInstanceOf(IllegalArgumentException.class);
    }
}
