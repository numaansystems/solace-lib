package com.numaansystems.solace.internal;

import com.numaansystems.solace.SolaceConfigurationException;

import java.time.Duration;

/**
 * Broker connection settings. A plain bean so it can be bound from Spring Boot
 * configuration properties. Holds no defaults for environment-specific values
 * (host, VPN, credentials, key material).
 */
public class SolaceConnectionSettings {

    /** How the client authenticates to the broker. */
    public enum AuthenticationScheme {
        /** Username and password. */
        BASIC,
        /** TLS client certificate from the configured key store. */
        CLIENT_CERTIFICATE
    }

    /**
     * Broker address, e.g. {@code tcps://broker.example.com:55443}. A
     * comma-separated list enables host failover.
     */
    private String host;

    /** Message VPN name. */
    private String vpnName;

    /** Client username. */
    private String username;

    /** Client password. */
    private String password;

    /** Authentication scheme. */
    private AuthenticationScheme authenticationScheme = AuthenticationScheme.BASIC;

    /**
     * Send NON_PERSISTENT messages as Solace direct messages. When false they
     * use the guaranteed transport. Must be false to use transacted consumers.
     */
    private boolean directTransport = false;

    /**
     * Let the client create queues and durable topic endpoints that do not
     * exist yet. Intended for development and tests; production endpoints
     * should be provisioned by broker administrators.
     */
    private boolean dynamicDurables = false;

    /** Timeout for one connection attempt. */
    private Duration connectTimeout = Duration.ofSeconds(30);

    /** Timeout for reading a reply from the broker. */
    private Duration readTimeout = Duration.ofSeconds(10);

    /** Interval between keep-alive messages. */
    private Duration keepAliveInterval = Duration.ofSeconds(3);

    /** Attempts to establish the initial connection after the first one fails; -1 retries forever. */
    private int connectRetries = 3;

    /** Attempts per host before moving to the next host in the list. */
    private int connectRetriesPerHost = 0;

    /** Attempts to transparently re-establish a lost connection; -1 retries forever. */
    private int reconnectRetries = 20;

    /** Wait between connection and reconnection attempts. */
    private Duration reconnectRetryWait = Duration.ofSeconds(3);

    /** ZLIB compression level 0-9; 0 disables compression. */
    private int compressionLevel = 0;

    private final Ssl ssl = new Ssl();

    /**
     * @throws SolaceConfigurationException if required settings are missing or inconsistent
     */
    public void validate() {
        require(host, "host");
        require(vpnName, "vpn-name");
        if (authenticationScheme == AuthenticationScheme.BASIC) {
            require(username, "username");
            require(password, "password");
        } else {
            require(ssl.getKeyStore(), "ssl.key-store");
            if (!usesTls()) {
                throw new SolaceConfigurationException(
                        "Client certificate authentication requires a tcps:// host");
            }
        }
        if (compressionLevel < 0 || compressionLevel > 9) {
            throw new SolaceConfigurationException("compression-level must be between 0 and 9");
        }
        if (connectRetries < -1 || reconnectRetries < -1 || connectRetriesPerHost < -1) {
            throw new SolaceConfigurationException("retry counts must be -1 (forever) or greater");
        }
    }

    private boolean usesTls() {
        return host != null && host.toLowerCase(java.util.Locale.ROOT).contains("tcps://");
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new SolaceConfigurationException("Solace connection setting '" + name + "' is required");
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVpnName() {
        return vpnName;
    }

    public void setVpnName(String vpnName) {
        this.vpnName = vpnName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public AuthenticationScheme getAuthenticationScheme() {
        return authenticationScheme;
    }

    public void setAuthenticationScheme(AuthenticationScheme authenticationScheme) {
        this.authenticationScheme = authenticationScheme;
    }

    public boolean isDirectTransport() {
        return directTransport;
    }

    public void setDirectTransport(boolean directTransport) {
        this.directTransport = directTransport;
    }

    public boolean isDynamicDurables() {
        return dynamicDurables;
    }

    public void setDynamicDurables(boolean dynamicDurables) {
        this.dynamicDurables = dynamicDurables;
    }

    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Duration getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Duration readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Duration getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(Duration keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public int getConnectRetries() {
        return connectRetries;
    }

    public void setConnectRetries(int connectRetries) {
        this.connectRetries = connectRetries;
    }

    public int getConnectRetriesPerHost() {
        return connectRetriesPerHost;
    }

    public void setConnectRetriesPerHost(int connectRetriesPerHost) {
        this.connectRetriesPerHost = connectRetriesPerHost;
    }

    public int getReconnectRetries() {
        return reconnectRetries;
    }

    public void setReconnectRetries(int reconnectRetries) {
        this.reconnectRetries = reconnectRetries;
    }

    public Duration getReconnectRetryWait() {
        return reconnectRetryWait;
    }

    public void setReconnectRetryWait(Duration reconnectRetryWait) {
        this.reconnectRetryWait = reconnectRetryWait;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public void setCompressionLevel(int compressionLevel) {
        this.compressionLevel = compressionLevel;
    }

    public Ssl getSsl() {
        return ssl;
    }

    @Override
    public String toString() {
        // Never include credentials or key-store passwords.
        return "SolaceConnectionSettings{host=" + host + ", vpnName=" + vpnName
                + ", authenticationScheme=" + authenticationScheme + '}';
    }

    /** TLS settings, used when the host uses the {@code tcps://} scheme. */
    public static class Ssl {

        /** Trust store location (file path or URL). Empty uses the JVM default. */
        private String trustStore;

        /** Trust store password. */
        private String trustStorePassword;

        /** Trust store format, e.g. JKS or PKCS12. */
        private String trustStoreFormat;

        /** Key store location, for client certificate authentication. */
        private String keyStore;

        /** Key store password. */
        private String keyStorePassword;

        /** Key store format, e.g. JKS or PKCS12. */
        private String keyStoreFormat;

        /** Validate the broker certificate chain. Disable only in test environments. */
        private boolean validateCertificate = true;

        /** Validate the broker certificate's validity dates. */
        private boolean validateCertificateDate = true;

        public String getTrustStore() {
            return trustStore;
        }

        public void setTrustStore(String trustStore) {
            this.trustStore = trustStore;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public void setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
        }

        public String getTrustStoreFormat() {
            return trustStoreFormat;
        }

        public void setTrustStoreFormat(String trustStoreFormat) {
            this.trustStoreFormat = trustStoreFormat;
        }

        public String getKeyStore() {
            return keyStore;
        }

        public void setKeyStore(String keyStore) {
            this.keyStore = keyStore;
        }

        public String getKeyStorePassword() {
            return keyStorePassword;
        }

        public void setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        public String getKeyStoreFormat() {
            return keyStoreFormat;
        }

        public void setKeyStoreFormat(String keyStoreFormat) {
            this.keyStoreFormat = keyStoreFormat;
        }

        public boolean isValidateCertificate() {
            return validateCertificate;
        }

        public void setValidateCertificate(boolean validateCertificate) {
            this.validateCertificate = validateCertificate;
        }

        public boolean isValidateCertificateDate() {
            return validateCertificateDate;
        }

        public void setValidateCertificateDate(boolean validateCertificateDate) {
            this.validateCertificateDate = validateCertificateDate;
        }
    }
}
