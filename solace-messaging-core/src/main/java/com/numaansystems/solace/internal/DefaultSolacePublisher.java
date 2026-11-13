package com.numaansystems.solace.internal;

import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.PublishResult;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceMessagingException;
import com.numaansystems.solace.SolacePublishException;
import com.numaansystems.solace.SolacePublisher;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.SessionCallback;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Publisher built on Spring's {@link JmsTemplate}. Every send runs on a small
 * pool of publisher threads so that blocking publishes can honour a timeout;
 * JMS itself offers none.
 */
public class DefaultSolacePublisher implements SolacePublisher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultSolacePublisher.class);

    private final JmsTemplate jmsTemplate;
    private final PayloadSerializer serializer;
    private final PublisherSettings settings;
    private final SolaceMetrics metrics;
    private final JmsMessageMapper mapper = new JmsMessageMapper();
    private final ExecutorService executor;

    /**
     * @param connectionFactory a factory that reuses connections and sessions,
     *                          e.g. {@link SolaceConnectionManager#getSharedConnectionFactory()}
     */
    public DefaultSolacePublisher(ConnectionFactory connectionFactory, PayloadSerializer serializer,
            PublisherSettings settings, SolaceMetrics metrics) {
        this.jmsTemplate = new JmsTemplate(Objects.requireNonNull(connectionFactory, "connectionFactory"));
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(settings.threads(), settings.threads(),
                60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(settings.queueCapacity()),
                runnable -> {
                    Thread thread = new Thread(runnable, "solace-publisher-" + threadNumber.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
        pool.allowCoreThreadTimeOut(true);
        this.executor = pool;
    }

    @Override
    public PublishResult publish(String destination, Object payload, PublishOptions options) {
        PublishOptions effective = options != null ? options : PublishOptions.defaults();
        Duration timeout = effective.getSendTimeout() != null ? effective.getSendTimeout() : settings.sendTimeout();
        CompletableFuture<PublishResult> future = publishAsync(destination, payload, effective);
        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            throw new SolacePublishException("Publish to '" + destination + "' did not complete within "
                    + timeout.toMillis() + " ms; the outcome is unknown", ex, true);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new SolacePublishException(
                    "Interrupted while publishing to '" + destination + "'; the outcome is unknown", ex, true);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof SolaceMessagingException messagingException) {
                throw messagingException;
            }
            throw new SolacePublishException("Publish to '" + destination + "' failed", cause, true);
        }
    }

    @Override
    public CompletableFuture<PublishResult> publishAsync(String destination, Object payload, PublishOptions options) {
        if (destination == null || destination.isBlank()) {
            throw new IllegalArgumentException("destination must not be blank");
        }
        Objects.requireNonNull(payload, "payload");
        PublishOptions effective = options != null ? options : PublishOptions.defaults();
        try {
            return CompletableFuture.supplyAsync(() -> doPublish(destination, payload, effective), executor);
        } catch (RejectedExecutionException ex) {
            return CompletableFuture.failedFuture(new SolacePublishException(
                    "Publish to '" + destination + "' rejected: the publisher is saturated or closed", ex, false));
        }
    }

    private PublishResult doPublish(String destination, Object payload, PublishOptions options) {
        // Serialization failures surface as SolaceSerializationException, before any send.
        SerializedPayload serialized = serializer.serialize(payload, options.getContentType());
        DeliveryMode deliveryMode = options.getDeliveryMode() != null
                ? options.getDeliveryMode()
                : settings.defaultDeliveryMode();
        String messageId = options.getMessageId() != null ? options.getMessageId() : UUID.randomUUID().toString();
        boolean dmqEligible = options.getDmqEligible() != null ? options.getDmqEligible() : settings.dmqEligible();

        int attempt = 0;
        while (true) {
            attempt++;
            AtomicBoolean sendAttempted = new AtomicBoolean(false);
            long start = System.nanoTime();
            try {
                PublishResult result = send(destination, serialized, options, deliveryMode, messageId, dmqEligible,
                        sendAttempted, attempt);
                metrics.recordPublish(options.getDestinationType(), deliveryMode, true, System.nanoTime() - start);
                log.debug("Published destination={} destinationType={} deliveryMode={} messageId={} correlationId={}",
                        destination, options.getDestinationType(), deliveryMode, messageId,
                        options.getCorrelationId());
                return result;
            } catch (RuntimeException ex) {
                metrics.recordPublish(options.getDestinationType(), deliveryMode, false, System.nanoTime() - start);
                boolean outcomeUnknown = sendAttempted.get();
                boolean retryable = !outcomeUnknown || settings.retryOnUnknownOutcome();
                if (retryable && attempt < settings.retry().maxAttempts() && pause(attempt)) {
                    metrics.recordPublishRetry();
                    log.warn("Publish attempt {} failed, retrying destination={} messageId={} correlationId={}: {}",
                            attempt, destination, messageId, options.getCorrelationId(), ex.toString());
                    continue;
                }
                throw new SolacePublishException("Publish to '" + destination + "' failed after " + attempt
                        + " attempt(s)" + (outcomeUnknown ? "; the outcome is unknown" : "; the message was not sent"),
                        ex, outcomeUnknown);
            }
        }
    }

    private PublishResult send(String destinationName, SerializedPayload payload, PublishOptions options,
            DeliveryMode deliveryMode, String messageId, boolean dmqEligible, AtomicBoolean sendAttempted,
            int attempt) {
        SessionCallback<PublishResult> callback = session -> {
            Message message = mapper.toJms(session, payload, options, messageId, dmqEligible);
            Destination destination =
                    JmsMessageMapper.destination(session, destinationName, options.getDestinationType());
            MessageProducer producer = session.createProducer(null);
            try {
                int jmsDeliveryMode = deliveryMode == DeliveryMode.PERSISTENT
                        ? jakarta.jms.DeliveryMode.PERSISTENT
                        : jakarta.jms.DeliveryMode.NON_PERSISTENT;
                int priority = options.getPriority() != null ? options.getPriority() : Message.DEFAULT_PRIORITY;
                long timeToLive = options.getTimeToLive() != null ? options.getTimeToLive().toMillis() : 0L;
                sendAttempted.set(true);
                producer.send(destination, message, jmsDeliveryMode, priority, timeToLive);
            } finally {
                producer.close();
            }
            long timestamp = message.getJMSTimestamp();
            return new PublishResult(destinationName, options.getDestinationType(), messageId,
                    message.getJMSMessageID(), deliveryMode,
                    timestamp > 0 ? Instant.ofEpochMilli(timestamp) : Instant.now(), attempt);
        };
        return jmsTemplate.execute(callback);
    }

    private boolean pause(int failedAttempt) {
        try {
            Thread.sleep(settings.retry().delayAfterAttempt(failedAttempt).toMillis());
            return true;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** Stops accepting publishes and waits briefly for queued ones to finish. */
    @Override
    public void close() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(settings.sendTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Publisher closed with publishes still in flight; their outcome is unknown");
                executor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
