package com.numaansystems.solace.internal;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SerializedPayload;
import com.numaansystems.solace.SolaceConsumeException;
import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolaceMessage;
import com.numaansystems.solace.SolaceMessageHandler;
import com.numaansystems.solace.SolaceSerializationException;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jms.listener.SessionAwareMessageListener;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/**
 * Per-registration listener: deserializes, invokes the handler with in-process
 * retries, then settles the message according to the acknowledgement mode.
 *
 * <p>Settlement rules (the listener container never acknowledges on its own):
 * <ul>
 *   <li>Success: AUTO acknowledges; MANUAL expects the handler to have
 *       acknowledged and otherwise requests redelivery; TRANSACTED returns so
 *       the container commits.</li>
 *   <li>Failure after all retries: the error handler runs; then the message is
 *       forwarded to the dead-letter queue and settled, or, without one,
 *       released for broker redelivery (session recover / transaction
 *       rollback).</li>
 * </ul>
 */
final class MessageProcessor<T> implements SessionAwareMessageListener<Message> {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    static final String MDC_CONSUMER = "solace.consumer";
    static final String MDC_MESSAGE_ID = "solace.messageId";
    static final String MDC_CORRELATION_ID = "solace.correlationId";

    private final String consumerName;
    private final AcknowledgeMode acknowledgeMode;
    private final RetryPolicy retryPolicy;
    private final String deadLetterQueue;
    private final Class<T> payloadType;
    private final SolaceMessageHandler<T> handler;
    private final PayloadSerializer serializer;
    private final JmsMessageMapper mapper;
    private final SolaceErrorHandler errorHandler;
    private final SolaceMetrics metrics;
    private final BooleanSupplier stopping;

    MessageProcessor(String consumerName, AcknowledgeMode acknowledgeMode, RetryPolicy retryPolicy,
            String deadLetterQueue, Class<T> payloadType, SolaceMessageHandler<T> handler,
            PayloadSerializer serializer, JmsMessageMapper mapper, SolaceErrorHandler errorHandler,
            SolaceMetrics metrics, BooleanSupplier stopping) {
        this.consumerName = consumerName;
        this.acknowledgeMode = acknowledgeMode;
        this.retryPolicy = retryPolicy;
        this.deadLetterQueue = deadLetterQueue;
        this.payloadType = payloadType;
        this.handler = handler;
        this.serializer = serializer;
        this.mapper = mapper;
        this.errorHandler = errorHandler;
        this.metrics = metrics;
        this.stopping = stopping;
    }

    @Override
    public void onMessage(Message jms, Session session) throws JMSException {
        long start = System.nanoTime();
        AtomicBoolean acknowledged = new AtomicBoolean(false);
        Runnable acknowledger = acknowledgeMode == AcknowledgeMode.MANUAL
                ? () -> acknowledge(jms, acknowledged)
                : null;

        SolaceMessage<Object> metadata = mapper.readMetadata(jms, acknowledger);
        metrics.recordReceived(consumerName, metadata.isRedelivered());

        MDC.put(MDC_CONSUMER, consumerName);
        putIfPresent(MDC_MESSAGE_ID, metadata.getMessageId());
        putIfPresent(MDC_CORRELATION_ID, metadata.getCorrelationId());
        try {
            SolaceMessage<T> message;
            try {
                SerializedPayload body = mapper.readBody(jms);
                message = metadata.withPayload(body == null ? null : serializer.deserialize(body, payloadType));
            } catch (SolaceSerializationException ex) {
                // Retrying cannot fix an undecodable payload.
                giveUp(jms, session, metadata, ex, acknowledged, start);
                return;
            }
            process(jms, session, message, acknowledged, start);
        } finally {
            MDC.remove(MDC_CONSUMER);
            MDC.remove(MDC_MESSAGE_ID);
            MDC.remove(MDC_CORRELATION_ID);
        }
    }

    private void process(Message jms, Session session, SolaceMessage<T> message, AtomicBoolean acknowledged,
            long start) throws JMSException {
        int attempt = 0;
        while (true) {
            attempt++;
            try {
                handler.onMessage(message);
                settleSuccess(jms, session, message, acknowledged);
                metrics.recordProcessed(consumerName, "success", System.nanoTime() - start);
                return;
            } catch (JMSException ex) {
                // Raised while settling, not by the handler: the session is suspect.
                throw ex;
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (acknowledged.get()) {
                    // MANUAL mode: the handler acknowledged and then failed. The
                    // message is gone from the broker, so there is nothing to retry.
                    log.error("Handler failed after acknowledging; message will not be redelivered", ex);
                    safeErrorHandler(message, ex);
                    metrics.recordProcessed(consumerName, "failure", System.nanoTime() - start);
                    return;
                }
                if (attempt < retryPolicy.maxAttempts()) {
                    log.warn("Handler attempt {} of {} failed, retrying: {}",
                            attempt, retryPolicy.maxAttempts(), ex.toString());
                    if (pause(retryPolicy.delayAfterAttempt(attempt))) {
                        metrics.recordProcessingRetry(consumerName);
                        continue;
                    }
                    // Shutting down: skip dead-lettering and let the broker redeliver later.
                    metrics.recordProcessed(consumerName, "failure", System.nanoTime() - start);
                    release(session, ex);
                    return;
                }
                giveUp(jms, session, message, ex, acknowledged, start);
                return;
            }
        }
    }

    private void settleSuccess(Message jms, Session session, SolaceMessage<T> message, AtomicBoolean acknowledged)
            throws JMSException {
        switch (acknowledgeMode) {
            case AUTO -> jms.acknowledge();
            case MANUAL -> {
                if (!acknowledged.get()) {
                    log.warn("Handler returned without acknowledging messageId={}; requesting redelivery",
                            message.getMessageId());
                    session.recover();
                }
            }
            case TRANSACTED -> {
                // The listener container commits when this listener returns.
            }
        }
    }

    private void giveUp(Message jms, Session session, SolaceMessage<?> message, Exception error,
            AtomicBoolean acknowledged, long start) throws JMSException {
        safeErrorHandler(message, error);
        if (deadLetterQueue != null && !acknowledged.get()) {
            try {
                MessageProducer producer = session.createProducer(session.createQueue(deadLetterQueue));
                try {
                    producer.send(jms, jakarta.jms.DeliveryMode.PERSISTENT, jms.getJMSPriority(), 0L);
                } finally {
                    producer.close();
                }
                if (acknowledgeMode != AcknowledgeMode.TRANSACTED) {
                    jms.acknowledge();
                }
                log.warn("Message moved to dead-letter queue {} messageId={}", deadLetterQueue,
                        message.getMessageId());
                metrics.recordProcessed(consumerName, "dead_lettered", System.nanoTime() - start);
                return;
            } catch (JMSException ex) {
                log.error("Could not move message to dead-letter queue {}; leaving it for redelivery",
                        deadLetterQueue, ex);
            }
        }
        metrics.recordProcessed(consumerName, "failure", System.nanoTime() - start);
        if (!acknowledged.get()) {
            release(session, error);
        }
    }

    /** Hand the message back to the broker for redelivery. */
    private void release(Session session, Exception error) throws JMSException {
        if (acknowledgeMode == AcknowledgeMode.TRANSACTED) {
            // Makes the listener container roll the transaction back.
            throw new SolaceConsumeException("Message processing failed; transaction rolled back", error);
        }
        session.recover();
    }

    private void acknowledge(Message jms, AtomicBoolean acknowledged) {
        try {
            jms.acknowledge();
            acknowledged.set(true);
        } catch (JMSException ex) {
            throw new SolaceConsumeException("Could not acknowledge message", ex);
        }
    }

    private void safeErrorHandler(SolaceMessage<?> message, Throwable error) {
        try {
            errorHandler.handle(consumerName, message, error);
        } catch (RuntimeException ex) {
            log.warn("Error handler threw; continuing", ex);
        }
    }

    /** Sleeps unless the consumer is stopping. Returns false if the wait was cut short. */
    private boolean pause(Duration delay) {
        long deadline = System.nanoTime() + delay.toNanos();
        try {
            while (true) {
                if (stopping.getAsBoolean()) {
                    return false;
                }
                long remainingMillis = (deadline - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    return true;
                }
                Thread.sleep(Math.min(remainingMillis, 100L));
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static void putIfPresent(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }
}
