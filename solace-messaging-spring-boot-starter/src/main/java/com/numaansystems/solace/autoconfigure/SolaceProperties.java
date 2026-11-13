package com.numaansystems.solace.autoconfigure;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.DeliveryMode;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SolaceConfigurationException;
import com.numaansystems.solace.internal.ConsumerSettings;
import com.numaansystems.solace.internal.PublisherSettings;
import com.numaansystems.solace.internal.SolaceConnectionSettings;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;

/**
 * Configuration for the Solace messaging library. Durations given as plain
 * numbers are milliseconds.
 */
@ConfigurationProperties(prefix = SolaceProperties.PREFIX)
public class SolaceProperties {

    public static final String PREFIX = "numaan.solace";

    /** Enable the Solace integration. */
    private boolean enabled = true;

    @NestedConfigurationProperty
    private final SolaceConnectionSettings connection = new SolaceConnectionSettings();

    private final Publisher publisher = new Publisher();

    private final Consumer consumer = new Consumer();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public SolaceConnectionSettings getConnection() {
        return connection;
    }

    public Publisher getPublisher() {
        return publisher;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public static class Publisher {

        /** Register the SolacePublisher bean. */
        private boolean enabled = true;

        /** Delivery mode used when a publish does not specify one. */
        private DeliveryMode defaultDeliveryMode = DeliveryMode.PERSISTENT;

        /** How long a blocking publish waits for the broker. */
        private Duration sendTimeout = Duration.ofSeconds(5);

        /** Mark published messages as eligible for the broker's dead message queue by default. */
        private boolean dmqEligible = true;

        /** Threads that perform sends. */
        private int threads = 4;

        /** Publishes that may queue for a publisher thread before new ones are rejected. */
        private int queueCapacity = 1000;

        private final Retry retry = new Retry(1);

        /**
         * Also retry a publish whose outcome is unknown (the broker may already
         * have the message). Can produce duplicates.
         */
        private boolean retryOnUnknownOutcome = false;

        PublisherSettings toSettings() {
            try {
                return new PublisherSettings(defaultDeliveryMode, sendTimeout, dmqEligible, retry.toPolicy(),
                        retryOnUnknownOutcome, threads, queueCapacity);
            } catch (RuntimeException ex) {
                throw new SolaceConfigurationException(
                        "Invalid " + PREFIX + ".publisher configuration: " + ex.getMessage(), ex);
            }
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public DeliveryMode getDefaultDeliveryMode() {
            return defaultDeliveryMode;
        }

        public void setDefaultDeliveryMode(DeliveryMode defaultDeliveryMode) {
            this.defaultDeliveryMode = defaultDeliveryMode;
        }

        public Duration getSendTimeout() {
            return sendTimeout;
        }

        public void setSendTimeout(Duration sendTimeout) {
            this.sendTimeout = sendTimeout;
        }

        public boolean isDmqEligible() {
            return dmqEligible;
        }

        public void setDmqEligible(boolean dmqEligible) {
            this.dmqEligible = dmqEligible;
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public Retry getRetry() {
            return retry;
        }

        public boolean isRetryOnUnknownOutcome() {
            return retryOnUnknownOutcome;
        }

        public void setRetryOnUnknownOutcome(boolean retryOnUnknownOutcome) {
            this.retryOnUnknownOutcome = retryOnUnknownOutcome;
        }
    }

    public static class Consumer {

        /** Register the SolaceConsumer bean. */
        private boolean enabled = true;

        /** Default number of concurrent consumers for queue registrations. */
        private int concurrency = 1;

        /** Default acknowledgement mode. */
        private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

        /** How long a consumer thread blocks waiting for a message. */
        private Duration receiveTimeout = Duration.ofSeconds(1);

        /** Wait between attempts to re-establish a failed consumer connection. */
        private Duration recoveryInterval = Duration.ofSeconds(5);

        private final Retry retry = new Retry(3);

        ConsumerSettings toSettings() {
            try {
                return new ConsumerSettings(concurrency, acknowledgeMode, retry.toPolicy(), receiveTimeout,
                        recoveryInterval);
            } catch (RuntimeException ex) {
                throw new SolaceConfigurationException(
                        "Invalid " + PREFIX + ".consumer configuration: " + ex.getMessage(), ex);
            }
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public AcknowledgeMode getAcknowledgeMode() {
            return acknowledgeMode;
        }

        public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
            this.acknowledgeMode = acknowledgeMode;
        }

        public Duration getReceiveTimeout() {
            return receiveTimeout;
        }

        public void setReceiveTimeout(Duration receiveTimeout) {
            this.receiveTimeout = receiveTimeout;
        }

        public Duration getRecoveryInterval() {
            return recoveryInterval;
        }

        public void setRecoveryInterval(Duration recoveryInterval) {
            this.recoveryInterval = recoveryInterval;
        }

        public Retry getRetry() {
            return retry;
        }
    }

    public static class Retry {

        /** Total attempts including the first; 1 disables retry. */
        private int maxAttempts;

        /** Delay before the second attempt. */
        private Duration initialInterval = Duration.ofSeconds(1);

        /** Factor applied to the delay after each attempt. */
        private double multiplier = 2.0;

        /** Upper bound for a single delay. */
        private Duration maxInterval = Duration.ofSeconds(10);

        public Retry() {
            this(1);
        }

        Retry(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        RetryPolicy toPolicy() {
            return new RetryPolicy(maxAttempts, initialInterval, multiplier, maxInterval);
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Duration getInitialInterval() {
            return initialInterval;
        }

        public void setInitialInterval(Duration initialInterval) {
            this.initialInterval = initialInterval;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public Duration getMaxInterval() {
            return maxInterval;
        }

        public void setMaxInterval(Duration maxInterval) {
            this.maxInterval = maxInterval;
        }
    }
}
