package com.numaansystems.solace.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolacePublisher;
import com.numaansystems.solace.internal.DefaultSolaceConsumer;
import com.numaansystems.solace.internal.DefaultSolacePublisher;
import com.numaansystems.solace.internal.JacksonPayloadSerializer;
import com.numaansystems.solace.internal.LoggingSolaceErrorHandler;
import com.numaansystems.solace.internal.SolaceConnectionManager;
import com.numaansystems.solace.internal.SolaceMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the Solace messaging library from {@code numaan.solace.*} properties.
 * Every bean can be replaced by declaring one of the same type.
 *
 * <p>The JMS {@code ConnectionFactory} is intentionally not exposed as a bean,
 * so it neither triggers Spring Boot's own JMS auto-configuration nor clashes
 * with another JMS provider in the same application.
 */
@AutoConfiguration(afterName = {
        "org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.MetricsAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration"})
@ConditionalOnClass(SolacePublisher.class)
@ConditionalOnProperty(prefix = SolaceProperties.PREFIX, name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(SolaceProperties.class)
public class SolaceAutoConfiguration {

    /** Validates connection settings at startup; does not connect yet. */
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public SolaceConnectionManager solaceConnectionManager(SolaceProperties properties) {
        return new SolaceConnectionManager(properties.getConnection());
    }

    @Bean
    @ConditionalOnMissingBean
    public PayloadSerializer solacePayloadSerializer(ObjectProvider<ObjectMapper> objectMapper) {
        return new JacksonPayloadSerializer(
                objectMapper.getIfUnique(() -> JsonMapper.builder().findAndAddModules().build()));
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceMetrics solaceMetrics(ObjectProvider<MeterRegistry> meterRegistry) {
        MeterRegistry registry = meterRegistry.getIfUnique();
        return registry != null ? new SolaceMetrics(registry) : SolaceMetrics.noop();
    }

    @Bean
    @ConditionalOnMissingBean
    public SolaceErrorHandler solaceErrorHandler() {
        return new LoggingSolaceErrorHandler();
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(SolacePublisher.class)
    @ConditionalOnProperty(prefix = SolaceProperties.PREFIX + ".publisher", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public DefaultSolacePublisher solacePublisher(SolaceConnectionManager connectionManager,
            PayloadSerializer serializer, SolaceMetrics metrics, SolaceProperties properties) {
        return new DefaultSolacePublisher(connectionManager.getSharedConnectionFactory(), serializer,
                properties.getPublisher().toSettings(), metrics);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(SolaceConsumer.class)
    @ConditionalOnProperty(prefix = SolaceProperties.PREFIX + ".consumer", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public DefaultSolaceConsumer solaceConsumer(SolaceConnectionManager connectionManager,
            PayloadSerializer serializer, SolaceMetrics metrics, SolaceErrorHandler errorHandler,
            SolaceProperties properties) {
        return new DefaultSolaceConsumer(connectionManager.getTargetConnectionFactory(),
                connectionManager.isDirectTransport(), serializer, properties.getConsumer().toSettings(), metrics,
                errorHandler);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({HealthIndicator.class, ConditionalOnEnabledHealthIndicator.class})
    static class SolaceHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "solaceHealthIndicator")
        @ConditionalOnEnabledHealthIndicator("solace")
        SolaceHealthIndicator solaceHealthIndicator(SolaceConnectionManager connectionManager,
                ObjectProvider<SolaceConsumer> consumer) {
            return new SolaceHealthIndicator(connectionManager, consumer.getIfAvailable());
        }
    }
}
