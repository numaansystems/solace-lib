package com.numaansystems.solace.internal;

import com.numaansystems.solace.SolaceConnectionException;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

import java.time.Instant;
import java.util.Objects;

/**
 * Owns the broker connection factories used by the library.
 *
 * <ul>
 *   <li>Publishing uses one shared connection with cached sessions and
 *       producers (JMS connections are thread-safe, sessions are not).</li>
 *   <li>Each consumer registration opens its own connection from the target
 *       factory, managed by its listener container.</li>
 * </ul>
 *
 * The Solace client reconnects transparently up to the configured number of
 * reconnect retries. If it gives up, the shared connection is discarded and
 * re-created on next use.
 */
public class SolaceConnectionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolaceConnectionManager.class);

    private final ConnectionFactory targetConnectionFactory;
    private final CachingConnectionFactory sharedConnectionFactory;
    private final boolean directTransport;

    private volatile String lastError;
    private volatile Instant lastErrorAt;

    public SolaceConnectionManager(SolaceConnectionSettings settings) {
        this(SolaceConnectionFactories.create(settings), settings.isDirectTransport());
    }

    /**
     * @param targetConnectionFactory a ready-to-use JMS connection factory
     * @param directTransport         whether that factory uses Solace direct transport
     */
    public SolaceConnectionManager(ConnectionFactory targetConnectionFactory, boolean directTransport) {
        this.targetConnectionFactory = Objects.requireNonNull(targetConnectionFactory, "targetConnectionFactory");
        this.directTransport = directTransport;
        this.sharedConnectionFactory = new CachingConnectionFactory(targetConnectionFactory);
        this.sharedConnectionFactory.setReconnectOnException(true);
        this.sharedConnectionFactory.setSessionCacheSize(16);
        this.sharedConnectionFactory.setExceptionListener(this::onConnectionException);
    }

    private void onConnectionException(JMSException ex) {
        this.lastError = ex.getMessage();
        this.lastErrorAt = Instant.now();
        log.error("Solace shared connection failed; it will be re-established on next use", ex);
    }

    /** Factory that creates a new broker connection per call; used by consumers. */
    public ConnectionFactory getTargetConnectionFactory() {
        return targetConnectionFactory;
    }

    /** Factory backed by the single shared connection; used for publishing. */
    public ConnectionFactory getSharedConnectionFactory() {
        return sharedConnectionFactory;
    }

    public boolean isDirectTransport() {
        return directTransport;
    }

    /**
     * Ensures the shared connection is established, connecting if necessary.
     *
     * @throws SolaceConnectionException if the broker cannot be reached
     */
    public void verifyConnection() {
        try {
            Connection connection = sharedConnectionFactory.createConnection();
            connection.close(); // returns the shared connection; does not disconnect
        } catch (JMSException ex) {
            this.lastError = ex.getMessage();
            this.lastErrorAt = Instant.now();
            throw new SolaceConnectionException("Could not connect to the Solace broker", ex);
        }
    }

    /** Message of the most recent connection failure, or {@code null}. */
    public String getLastError() {
        return lastError;
    }

    public Instant getLastErrorAt() {
        return lastErrorAt;
    }

    @Override
    public void close() {
        sharedConnectionFactory.destroy();
    }
}
