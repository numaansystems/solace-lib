package com.numaansystems.solace;

import tools.jackson.databind.json.JsonMapper;
import com.numaansystems.solace.internal.ConsumerSettings;
import com.numaansystems.solace.internal.DefaultSolaceConsumer;
import com.numaansystems.solace.internal.DefaultSolacePublisher;
import com.numaansystems.solace.internal.JacksonPayloadSerializer;
import com.numaansystems.solace.internal.LoggingSolaceErrorHandler;
import com.numaansystems.solace.internal.PublisherSettings;
import com.numaansystems.solace.internal.SolaceConnectionManager;
import com.numaansystems.solace.internal.SolaceConnectionSettings;
import com.numaansystems.solace.internal.SolaceMetrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.Service;
import org.testcontainers.solace.SolaceContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Broker-based tests against a real Solace PubSub+ container. Run with
 * {@code mvn verify -Pintegration-test}; requires Docker.
 *
 * <p>Queues and durable topic endpoints are created on demand through the
 * client's "dynamic durables" feature, which the library leaves off by default.
 * Queues created that way are exclusive, so the competing-consumer test checks
 * once-only processing but cannot check that load is spread across consumers.
 *
 * <p>Not covered here: recovery after a broker restart or network cut. That
 * needs fault injection (for example Toxiproxy) and is exercised manually; see
 * the README.
 */
@Testcontainers
class SolaceBrokerIT {

    private static final String IMAGE =
            System.getProperty("solace.image", "solace/solace-pubsub-standard:latest");
    private static final long WAIT_SECONDS = 20;

    @Container
    static final SolaceContainer BROKER = new SolaceContainer(IMAGE)
            .withCredentials("it-user", "it-password");

    public record Quote(String symbol, double price) {
    }

    private static SolaceConnectionManager connections;
    private static DefaultSolacePublisher publisher;
    private static DefaultSolaceConsumer consumer;

    @BeforeAll
    static void connect() {
        SolaceConnectionSettings settings = new SolaceConnectionSettings();
        settings.setHost(BROKER.getOrigin(Service.SMF));
        settings.setVpnName(BROKER.getVpn());
        settings.setUsername(BROKER.getUsername());
        settings.setPassword(BROKER.getPassword());
        settings.setDynamicDurables(true);

        JacksonPayloadSerializer serializer = new JacksonPayloadSerializer(JsonMapper.builder().build());
        connections = new SolaceConnectionManager(settings);
        publisher = new DefaultSolacePublisher(connections.getSharedConnectionFactory(), serializer,
                PublisherSettings.defaults(), SolaceMetrics.noop());
        consumer = new DefaultSolaceConsumer(connections.getTargetConnectionFactory(), false, serializer,
                ConsumerSettings.defaults(), SolaceMetrics.noop(), new LoggingSolaceErrorHandler());
        consumer.start();
    }

    @AfterAll
    static void disconnect() {
        consumer.close();
        publisher.close();
        connections.close();
    }

    private static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private static PublishOptions toQueue() {
        return PublishOptions.builder().destinationType(DestinationType.QUEUE).build();
    }

    @Test
    void persistentMessageToQueueKeepsPayloadAndMetadata() throws Exception {
        String queue = unique("it.meta");
        BlockingQueue<SolaceMessage<Quote>> received = new LinkedBlockingQueue<>();

        PublishResult result = publisher.publish(queue, new Quote("EURUSD", 1.08), PublishOptions.builder()
                .destinationType(DestinationType.QUEUE)
                .deliveryMode(DeliveryMode.PERSISTENT)
                .correlationId("REQ-123456")
                .replyTo("pricing/response/clientA")
                .messageId("quote-1")
                .property("tenant", "acme")
                .build());

        try (ConsumerRegistration ignored = consumer.register(ConsumerOptions.queue(queue).build(),
                Quote.class, received::add)) {
            SolaceMessage<Quote> message = received.poll(WAIT_SECONDS, TimeUnit.SECONDS);

            assertThat(message).isNotNull();
            assertThat(message.getPayload()).isEqualTo(new Quote("EURUSD", 1.08));
            assertThat(message.getMessageId()).isEqualTo("quote-1").isEqualTo(result.messageId());
            assertThat(message.getCorrelationId()).isEqualTo("REQ-123456");
            assertThat(message.getReplyTo()).isEqualTo("pricing/response/clientA");
            assertThat(message.getReplyToType()).isEqualTo(DestinationType.TOPIC);
            assertThat(message.getContentType()).isEqualTo("application/json");
            assertThat(message.getDeliveryMode()).isEqualTo(DeliveryMode.PERSISTENT);
            assertThat(message.getProperties()).containsEntry("tenant", "acme");
            assertThat(message.isRedelivered()).isFalse();
        }
    }

    @Test
    void nonPersistentMessageIsDelivered() throws Exception {
        String queue = unique("it.nonpersistent");
        BlockingQueue<SolaceMessage<Object>> received = new LinkedBlockingQueue<>();

        try (ConsumerRegistration ignored = consumer.register(ConsumerOptions.queue(queue).build(), received::add)) {
            publisher.publish(queue, "tick", PublishOptions.builder()
                    .destinationType(DestinationType.QUEUE)
                    .deliveryMode(DeliveryMode.NON_PERSISTENT)
                    .build());

            SolaceMessage<Object> message = received.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getPayload()).isEqualTo("tick");
            assertThat(message.getDeliveryMode()).isEqualTo(DeliveryMode.NON_PERSISTENT);
        }
    }

    @Test
    void durableSubscriptionReceivesMessagesPublishedWhileOffline() throws Exception {
        String topic = "it/durable/" + unique("t");
        String subscription = unique("it-durable-sub");
        BlockingQueue<SolaceMessage<Object>> received = new LinkedBlockingQueue<>();
        ConsumerOptions options = ConsumerOptions.durableTopic(topic, subscription).build();

        // First bind creates the durable endpoint and its subscription.
        ConsumerRegistration first = consumer.register(options, received::add);
        publishUntilReceived(topic, received);
        first.close();
        received.clear();

        publisher.publish(topic, "while-offline");

        try (ConsumerRegistration ignored = consumer.register(options, received::add)) {
            SolaceMessage<Object> message = received.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getPayload()).isEqualTo("while-offline");
        }
    }

    @Test
    void nonDurableSubscriptionMissesMessagesPublishedWhileOffline() throws Exception {
        String topic = "it/nondurable/" + unique("t");
        BlockingQueue<SolaceMessage<Object>> received = new LinkedBlockingQueue<>();

        publisher.publish(topic, "before-subscribing");

        try (ConsumerRegistration ignored = consumer.register(ConsumerOptions.topic(topic).build(), received::add)) {
            List<Object> payloads = publishUntilReceived(topic, received);
            assertThat(payloads).doesNotContain("before-subscribing");
        }
    }

    @Test
    void competingConsumersEachMessageProcessedOnce() throws Exception {
        String queue = unique("it.competing");
        int messages = 60;
        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(messages);

        try (ConsumerRegistration ignored = consumer.register(
                ConsumerOptions.queue(queue).concurrency(3).build(), message -> {
                    Thread.sleep(20);
                    deliveries.incrementAndGet();
                    if (seen.add(String.valueOf(message.getPayload()))) {
                        done.countDown();
                    }
                })) {
            for (int i = 0; i < messages; i++) {
                publisher.publish(queue, "m-" + i, toQueue());
            }

            assertThat(done.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(deliveries).hasValue(messages);
        }
    }

    @Test
    void failedMessageIsRedelivered() throws Exception {
        String queue = unique("it.redelivery");
        BlockingQueue<Boolean> redeliveredFlags = new LinkedBlockingQueue<>();
        AtomicBoolean failedOnce = new AtomicBoolean();

        try (ConsumerRegistration ignored = consumer.register(
                ConsumerOptions.queue(queue).retryPolicy(RetryPolicy.none()).build(), message -> {
                    redeliveredFlags.add(message.isRedelivered());
                    if (failedOnce.compareAndSet(false, true)) {
                        throw new IllegalStateException("first delivery fails");
                    }
                })) {
            publisher.publish(queue, "retry-me", toQueue());

            assertThat(redeliveredFlags.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isFalse();
            assertThat(redeliveredFlags.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(redeliveredFlags.poll(2, TimeUnit.SECONDS)).isNull();
        }
    }

    @Test
    void exhaustedRetriesMoveMessageToDeadLetterQueue() throws Exception {
        String queue = unique("it.poison");
        String deadLetterQueue = unique("it.poison.dlq");
        AtomicInteger attempts = new AtomicInteger();
        BlockingQueue<SolaceMessage<Object>> deadLetters = new LinkedBlockingQueue<>();

        try (ConsumerRegistration failing = consumer.register(
                ConsumerOptions.queue(queue)
                        .retryPolicy(new RetryPolicy(2, Duration.ofMillis(50), 1.0, Duration.ofMillis(50)))
                        .deadLetterQueue(deadLetterQueue)
                        .build(),
                message -> {
                    attempts.incrementAndGet();
                    throw new IllegalStateException("always fails");
                });
                ConsumerRegistration dlq = consumer.register(
                        ConsumerOptions.queue(deadLetterQueue).build(), deadLetters::add)) {
            publisher.publish(queue, "poison", PublishOptions.builder()
                    .destinationType(DestinationType.QUEUE)
                    .correlationId("POISON-1")
                    .build());

            SolaceMessage<Object> deadLetter = deadLetters.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(deadLetter).isNotNull();
            assertThat(deadLetter.getPayload()).isEqualTo("poison");
            assertThat(deadLetter.getCorrelationId()).isEqualTo("POISON-1");
            assertThat(attempts).hasValue(2);
            assertThat(deadLetters.poll(2, TimeUnit.SECONDS)).isNull();
        }
    }

    @Test
    void manualAcknowledgementAndTransactedConsumption() throws Exception {
        String manualQueue = unique("it.manual");
        String transactedQueue = unique("it.tx");
        BlockingQueue<Object> manual = new LinkedBlockingQueue<>();
        BlockingQueue<Boolean> transacted = new LinkedBlockingQueue<>();
        AtomicBoolean rolledBackOnce = new AtomicBoolean();

        try (ConsumerRegistration first = consumer.register(
                ConsumerOptions.queue(manualQueue).acknowledgeMode(AcknowledgeMode.MANUAL).build(), message -> {
                    message.acknowledge();
                    manual.add(message.getPayload());
                });
                ConsumerRegistration second = consumer.register(
                        ConsumerOptions.queue(transactedQueue).acknowledgeMode(AcknowledgeMode.TRANSACTED)
                                .retryPolicy(RetryPolicy.none()).build(), message -> {
                            transacted.add(message.isRedelivered());
                            if (rolledBackOnce.compareAndSet(false, true)) {
                                throw new IllegalStateException("roll back once");
                            }
                        })) {
            publisher.publish(manualQueue, "ack-me", toQueue());
            publisher.publish(transactedQueue, "tx", toQueue());

            assertThat(manual.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo("ack-me");
            assertThat(manual.poll(2, TimeUnit.SECONDS)).isNull();
            assertThat(transacted.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isFalse();
            assertThat(transacted.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void replyIsPublishedToReplyToWithTheSameCorrelationId() throws Exception {
        String requestQueue = unique("it.request");
        String replyTopic = "it/reply/" + unique("client");
        BlockingQueue<SolaceMessage<Object>> replies = new LinkedBlockingQueue<>();

        try (ConsumerRegistration responder = consumer.register(
                ConsumerOptions.queue(requestQueue).build(), String.class, request ->
                        publisher.publish(request.getReplyTo(), "re:" + request.getPayload(),
                                PublishOptions.builder()
                                        .destinationType(request.getReplyToType())
                                        .correlationId(request.getCorrelationId())
                                        .build()));
                ConsumerRegistration replyListener = consumer.register(
                        ConsumerOptions.topic(replyTopic).build(), replies::add)) {
            publishUntilReceived(replyTopic, replies); // the reply subscription is now active
            replies.clear();

            publisher.publish(requestQueue, "price?", PublishOptions.builder()
                    .destinationType(DestinationType.QUEUE)
                    .correlationId("REQ-777")
                    .replyTo(replyTopic)
                    .build());

            SolaceMessage<Object> reply = replies.poll(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(reply).isNotNull();
            assertThat(reply.getPayload()).isEqualTo("re:price?");
            assertThat(reply.getCorrelationId()).isEqualTo("REQ-777");
        }
    }

    @Test
    void concurrentAsyncPublishesAllArrive() throws Exception {
        String queue = unique("it.concurrent");
        int messages = 200;
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        CountDownLatch done = new CountDownLatch(messages);

        List<CompletableFuture<PublishResult>> futures = new ArrayList<>();
        for (int i = 0; i < messages; i++) {
            futures.add(publisher.publishAsync(queue, "c-" + i, toQueue()));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(WAIT_SECONDS, TimeUnit.SECONDS);

        try (ConsumerRegistration ignored = consumer.register(
                ConsumerOptions.queue(queue).concurrency(2).build(), message -> {
                    if (seen.add(message.getPayload())) {
                        done.countDown();
                    }
                })) {
            assertThat(done.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void closingARegistrationWaitsForTheInFlightHandler() throws Exception {
        String queue = unique("it.shutdown");
        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean finished = new AtomicBoolean();
        AtomicInteger deliveries = new AtomicInteger();

        ConsumerRegistration registration = consumer.register(ConsumerOptions.queue(queue).build(), message -> {
            deliveries.incrementAndGet();
            started.countDown();
            Thread.sleep(1_500);
            finished.set(true);
        });
        publisher.publish(queue, "slow", toQueue());
        assertThat(started.await(WAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

        registration.close();

        assertThat(finished).isTrue();
        assertThat(registration.isRunning()).isFalse();

        // The message was acknowledged before shutdown, so a new consumer sees nothing.
        try (ConsumerRegistration ignored = consumer.register(
                ConsumerOptions.queue(queue).name(unique("after")).build(),
                message -> deliveries.incrementAndGet())) {
            Thread.sleep(2_000);
            assertThat(deliveries).hasValue(1);
        }
    }

    /**
     * A topic subscription becomes active asynchronously after registration.
     * Publishes probe messages until one arrives and returns every payload seen.
     */
    private static List<Object> publishUntilReceived(String topic, BlockingQueue<SolaceMessage<Object>> received)
            throws InterruptedException {
        List<Object> payloads = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
        int probe = 0;
        while (System.nanoTime() < deadline) {
            publisher.publish(topic, "probe-" + probe++);
            SolaceMessage<Object> message = received.poll(250, TimeUnit.MILLISECONDS);
            if (message != null) {
                payloads.add(message.getPayload());
                received.drainTo(new ArrayList<>());
                return payloads;
            }
        }
        throw new AssertionError("No message received on " + topic + " within " + WAIT_SECONDS + "s");
    }
}
