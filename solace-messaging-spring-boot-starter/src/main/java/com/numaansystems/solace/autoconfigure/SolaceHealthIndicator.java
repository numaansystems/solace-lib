package com.numaansystems.solace.autoconfigure;

import com.numaansystems.solace.ConsumerRegistration;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.internal.SolaceConnectionManager;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.Collection;

/**
 * Reports whether the shared broker connection is available. While the Solace
 * client is transparently reconnecting the connection still counts as up; it
 * is reported down once the client has given up and a fresh connection attempt
 * fails. Details never include host, username or credentials.
 */
public class SolaceHealthIndicator implements HealthIndicator {

    private final SolaceConnectionManager connectionManager;
    private final SolaceConsumer consumer;

    public SolaceHealthIndicator(SolaceConnectionManager connectionManager, SolaceConsumer consumer) {
        this.connectionManager = connectionManager;
        this.consumer = consumer;
    }

    @Override
    public Health health() {
        try {
            connectionManager.verifyConnection();
        } catch (RuntimeException ex) {
            return Health.down(ex).build();
        }
        Health.Builder builder = Health.up();
        if (consumer != null) {
            Collection<ConsumerRegistration> registrations = consumer.getRegistrations();
            builder.withDetail("consumers", registrations.size());
            builder.withDetail("consumersRunning",
                    registrations.stream().filter(ConsumerRegistration::isRunning).count());
        }
        if (connectionManager.getLastErrorAt() != null) {
            builder.withDetail("lastConnectionErrorAt", connectionManager.getLastErrorAt().toString());
        }
        return builder.build();
    }
}
