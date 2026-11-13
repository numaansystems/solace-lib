package com.numaansystems.solace.internal;

import com.numaansystems.solace.AcknowledgeMode;
import com.numaansystems.solace.ConsumerOptions;
import com.numaansystems.solace.ConsumerRegistration;
import com.numaansystems.solace.DestinationType;
import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.RetryPolicy;
import com.numaansystems.solace.SolaceConfigurationException;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolaceMessageHandler;
import com.numaansystems.solace.SubscriptionType;
import io.micrometer.core.instrument.Meter;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Consumer built on Spring's {@link DefaultMessageListenerContainer}: one
 * container (and one broker connection) per registration. The container
 * supplies concurrency, connection recovery and graceful shutdown.
 *
 * <p>As a {@link SmartLifecycle} bean it starts delivery once the application
 * context is ready and stops it, waiting for in-flight handlers, before other
 * beans (such as the publisher) are destroyed. Outside Spring, call
 * {@link #start()} and {@link #close()} yourself.
 */
public class DefaultSolaceConsumer implements SolaceConsumer, SmartLifecycle, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DefaultSolaceConsumer.class);

    private final ConnectionFactory connectionFactory;
    private final boolean directTransport;
    private final PayloadSerializer serializer;
    private final ConsumerSettings settings;
    private final SolaceMetrics metrics;
    private final SolaceErrorHandler defaultErrorHandler;
    private final JmsMessageMapper mapper = new JmsMessageMapper();
    private final Map<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Object lifecycleMonitor = new Object();
    private boolean running;
    private boolean closed;

    /**
     * @param connectionFactory factory that opens a new broker connection per
     *                          call, e.g. {@link SolaceConnectionManager#getTargetConnectionFactory()}
     * @param directTransport   whether that factory uses Solace direct transport
     */
    public DefaultSolaceConsumer(ConnectionFactory connectionFactory, boolean directTransport,
            PayloadSerializer serializer, ConsumerSettings settings, SolaceMetrics metrics,
            SolaceErrorHandler defaultErrorHandler) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        this.directTransport = directTransport;
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.defaultErrorHandler = Objects.requireNonNull(defaultErrorHandler, "defaultErrorHandler");
    }

    @Override
    public <T> ConsumerRegistration register(ConsumerOptions options, Class<T> payloadType,
            SolaceMessageHandler<T> handler) {
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(payloadType, "payloadType");
        Objects.requireNonNull(handler, "handler");

        boolean topic = options.getDestinationType() == DestinationType.TOPIC;
        AcknowledgeMode acknowledgeMode =
                options.getAcknowledgeMode() != null ? options.getAcknowledgeMode() : settings.acknowledgeMode();
        if (acknowledgeMode == AcknowledgeMode.TRANSACTED && directTransport) {
            throw new SolaceConfigurationException(
                    "Transacted consumers are not supported with direct transport; set direct-transport=false");
        }
        RetryPolicy retryPolicy = options.getRetryPolicy() != null ? options.getRetryPolicy() : settings.retry();
        int concurrency = topic ? 1 : (options.getConcurrency() != null ? options.getConcurrency() : settings.concurrency());
        int maxConcurrency = topic ? 1 : Math.max(concurrency,
                options.getMaxConcurrency() != null ? options.getMaxConcurrency() : concurrency);
        String name = options.getName() != null ? options.getName() : defaultName(options);
        SolaceErrorHandler errorHandler =
                options.getErrorHandler() != null ? options.getErrorHandler() : defaultErrorHandler;

        AtomicBoolean stopping = new AtomicBoolean(false);
        MessageProcessor<T> processor = new MessageProcessor<>(name, acknowledgeMode, retryPolicy,
                options.getDeadLetterQueue(), payloadType, handler, serializer, mapper, errorHandler, metrics,
                stopping::get);

        LibraryAcknowledgedContainer container = new LibraryAcknowledgedContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(options.getDestination());
        container.setPubSubDomain(topic);
        if (options.getSubscriptionType() == SubscriptionType.DURABLE) {
            container.setSubscriptionDurable(true);
            container.setSubscriptionName(options.getSubscriptionName());
        }
        if (acknowledgeMode == AcknowledgeMode.TRANSACTED) {
            container.setSessionTransacted(true);
        } else {
            container.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        }
        container.setConcurrentConsumers(concurrency);
        container.setMaxConcurrentConsumers(maxConcurrency);
        container.setReceiveTimeout(settings.receiveTimeout().toMillis());
        container.setBackOff(new FixedBackOff(settings.recoveryInterval().toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS));
        if (options.getMessageSelector() != null) {
            container.setMessageSelector(options.getMessageSelector());
        }
        container.setMessageListener(processor);
        container.setErrorHandler(error ->
                log.error("Consumer {} could not settle a message; it will be redelivered", name, error));
        container.setBeanName(name);
        container.setAutoStartup(false);

        Registration registration = new Registration(name, container, stopping, options.isAutoStartup());
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("Consumer has been closed");
            }
            if (registrations.putIfAbsent(name, registration) != null) {
                throw new SolaceConfigurationException("A consumer named '" + name + "' is already registered");
            }
            try {
                container.afterPropertiesSet();
                registration.gauge = metrics.registerConcurrencyGauge(name, container::getActiveConsumerCount);
                if (running && options.isAutoStartup()) {
                    container.start();
                }
            } catch (RuntimeException ex) {
                registrations.remove(name);
                registration.shutdownQuietly();
                throw ex;
            }
        }
        log.info("Registered consumer name={} destination={} destinationType={} subscriptionType={} "
                        + "acknowledgeMode={} concurrency={}-{}",
                name, options.getDestination(), options.getDestinationType(), options.getSubscriptionType(),
                acknowledgeMode, concurrency, maxConcurrency);
        return registration;
    }

    private static String defaultName(ConsumerOptions options) {
        if (options.getSubscriptionType() == SubscriptionType.DURABLE) {
            return "durable:" + options.getSubscriptionName();
        }
        return options.getDestinationType().name().toLowerCase(java.util.Locale.ROOT) + ":" + options.getDestination();
    }

    @Override
    public Collection<ConsumerRegistration> getRegistrations() {
        return List.copyOf(registrations.values());
    }

    /** Starts delivery for every registration that has auto-startup enabled. */
    @Override
    public void start() {
        synchronized (lifecycleMonitor) {
            if (closed) {
                throw new IllegalStateException("Consumer has been closed");
            }
            running = true;
            for (Registration registration : registrations.values()) {
                if (registration.autoStartup) {
                    registration.start();
                }
            }
        }
    }

    /** Same as {@link #close()}: a stopped consumer is not restartable. */
    @Override
    public void stop() {
        close();
    }

    @Override
    public boolean isRunning() {
        synchronized (lifecycleMonitor) {
            return running;
        }
    }

    /** Start after, and stop before, ordinary application beans. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1000;
    }

    /**
     * Stops all registrations, waiting for in-flight handlers to return, and
     * releases their connections.
     */
    @Override
    public void close() {
        List<Registration> toClose;
        synchronized (lifecycleMonitor) {
            running = false;
            closed = true;
            toClose = new ArrayList<>(registrations.values());
        }
        toClose.forEach(Registration::close);
    }

    /**
     * Listener container that leaves acknowledgement to {@link MessageProcessor},
     * so that every acknowledge and recover decision is made in one place.
     */
    private static final class LibraryAcknowledgedContainer extends DefaultMessageListenerContainer {

        @Override
        protected boolean isClientAcknowledge(Session session) {
            return false;
        }
    }

    private final class Registration implements ConsumerRegistration {

        private final String name;
        private final DefaultMessageListenerContainer container;
        private final AtomicBoolean stopping;
        private final boolean autoStartup;
        private final AtomicBoolean closedFlag = new AtomicBoolean(false);
        private volatile Meter gauge;

        private Registration(String name, DefaultMessageListenerContainer container, AtomicBoolean stopping,
                boolean autoStartup) {
            this.name = name;
            this.container = container;
            this.stopping = stopping;
            this.autoStartup = autoStartup;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void start() {
            if (closedFlag.get()) {
                throw new IllegalStateException("Consumer registration '" + name + "' has been closed");
            }
            stopping.set(false);
            container.start();
        }

        @Override
        public void stop() {
            stopping.set(true);
            container.stop();
        }

        @Override
        public boolean isRunning() {
            return !closedFlag.get() && container.isRunning();
        }

        @Override
        public boolean isReady() {
            return isRunning() && container.isRegisteredWithDestination();
        }

        @Override
        public void close() {
            if (closedFlag.compareAndSet(false, true)) {
                registrations.remove(name, this);
                shutdownQuietly();
                log.info("Closed consumer name={}", name);
            }
        }

        private void shutdownQuietly() {
            stopping.set(true);
            try {
                container.shutdown(); // blocks until in-flight handlers have returned
            } catch (RuntimeException ex) {
                log.warn("Error while shutting down consumer {}", name, ex);
            }
            metrics.remove(gauge);
        }
    }
}
