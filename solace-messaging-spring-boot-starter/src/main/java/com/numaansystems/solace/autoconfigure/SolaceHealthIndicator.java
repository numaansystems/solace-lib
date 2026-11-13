package com.numaansystems.solace.autoconfigure;

import com.numaansystems.solace.ConsumerRegistration;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.internal.SolaceConnectionManager;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.Collection;

/**
 * Reports whether the shared broker connection is available. While the Solace
 * client is transparently reconnecting the connection still counts as up; it
 * is reported down once the client has given up and a fresh connection attempt
 * fails. Details never include host, username or credentials.
 */
public class SolaceHealthIndicator extends AbstractHealthIndicator {

    private final SolaceConnectionManager connectionManager;
    private final SolaceConsumer consumer;

    public SolaceHealthIndicator(SolaceConnectionManager connectionManager, SolaceConsumer consumer) {
        super("Solace health check failed");
        this.connectionManager = connectionManager;
        this.consumer = consumer;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) {
        connectionManager.verifyConnection();
        builder.up();
        if (consumer != null) {
            Collection<ConsumerRegistration> registrations = consumer.getRegistrations();
            builder.withDetail("consumers", registrations.size());
            builder.withDetail("consumersRunning",
                    registrations.stream().filter(ConsumerRegistration::isRunning).count());
        }
        if (connectionManager.getLastErrorAt() != null) {
            builder.withDetail("lastConnectionErrorAt", connectionManager.getLastErrorAt().toString());
        }
    }
}
