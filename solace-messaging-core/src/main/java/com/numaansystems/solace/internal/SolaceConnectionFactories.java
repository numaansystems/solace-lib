package com.numaansystems.solace.internal;

import com.numaansystems.solace.SolaceConfigurationException;
import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.SupportedProperty;
import jakarta.jms.ConnectionFactory;

/**
 * Builds the Solace JMS connection factory from {@link SolaceConnectionSettings}.
 * Creating the factory does not open a connection.
 */
public final class SolaceConnectionFactories {

    private SolaceConnectionFactories() {
    }

    public static ConnectionFactory create(SolaceConnectionSettings settings) {
        settings.validate();
        try {
            SolConnectionFactory factory = SolJmsUtility.createConnectionFactory();
            factory.setHost(settings.getHost());
            factory.setVPN(settings.getVpnName());
            if (settings.getUsername() != null) {
                factory.setUsername(settings.getUsername());
            }
            if (settings.getPassword() != null) {
                factory.setPassword(settings.getPassword());
            }
            factory.setAuthenticationScheme(
                    settings.getAuthenticationScheme() == SolaceConnectionSettings.AuthenticationScheme.CLIENT_CERTIFICATE
                            ? SupportedProperty.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE
                            : SupportedProperty.AUTHENTICATION_SCHEME_BASIC);
            factory.setDirectTransport(settings.isDirectTransport());
            factory.setDynamicDurables(settings.isDynamicDurables());
            factory.setConnectTimeoutInMillis(millis(settings.getConnectTimeout()));
            factory.setReadTimeoutInMillis(millis(settings.getReadTimeout()));
            factory.setKeepAliveIntervalInMillis(millis(settings.getKeepAliveInterval()));
            factory.setConnectRetries(settings.getConnectRetries());
            factory.setConnectRetriesPerHost(settings.getConnectRetriesPerHost());
            factory.setReconnectRetries(settings.getReconnectRetries());
            factory.setReconnectRetryWaitInMillis(millis(settings.getReconnectRetryWait()));
            if (settings.getCompressionLevel() > 0) {
                factory.setCompressionLevel(settings.getCompressionLevel());
            }

            SolaceConnectionSettings.Ssl ssl = settings.getSsl();
            factory.setSSLValidateCertificate(ssl.isValidateCertificate());
            factory.setSSLValidateCertificateDate(ssl.isValidateCertificateDate());
            if (hasText(ssl.getTrustStore())) {
                factory.setSSLTrustStore(ssl.getTrustStore());
            }
            if (hasText(ssl.getTrustStorePassword())) {
                factory.setSSLTrustStorePassword(ssl.getTrustStorePassword());
            }
            if (hasText(ssl.getTrustStoreFormat())) {
                factory.setSSLTrustStoreFormat(ssl.getTrustStoreFormat());
            }
            if (hasText(ssl.getKeyStore())) {
                factory.setSSLKeyStore(ssl.getKeyStore());
            }
            if (hasText(ssl.getKeyStorePassword())) {
                factory.setSSLKeyStorePassword(ssl.getKeyStorePassword());
            }
            if (hasText(ssl.getKeyStoreFormat())) {
                factory.setSSLKeyStoreFormat(ssl.getKeyStoreFormat());
            }
            return factory;
        } catch (SolaceConfigurationException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SolaceConfigurationException("Could not create the Solace connection factory", ex);
        }
    }

    private static int millis(java.time.Duration duration) {
        return Math.toIntExact(duration.toMillis());
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
