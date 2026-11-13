package com.numaansystems.solace.autoconfigure;

import com.numaansystems.solace.PayloadSerializer;
import com.numaansystems.solace.PublishOptions;
import com.numaansystems.solace.PublishResult;
import com.numaansystems.solace.SolaceConfigurationException;
import com.numaansystems.solace.SolaceConsumer;
import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolacePublisher;
import com.numaansystems.solace.internal.DefaultSolacePublisher;
import com.numaansystems.solace.internal.SolaceConnectionManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Creating the connection factory does not open a connection, so these tests
 * need no broker.
 */
class SolaceAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(SolaceAutoConfiguration.class));

    private static final String[] CONNECTION = {
            "numaan.solace.connection.host=tcp://localhost:55555",
            "numaan.solace.connection.vpn-name=default",
            "numaan.solace.connection.username=user",
            "numaan.solace.connection.password=secret"};

    @Test
    void registersPublisherAndConsumerFromConnectionPropertiesAlone() {
        contextRunner.withPropertyValues(CONNECTION).run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(SolacePublisher.class);
            assertThat(context).hasSingleBean(SolaceConsumer.class);
            assertThat(context).hasSingleBean(PayloadSerializer.class);
            assertThat(context).hasSingleBean(SolaceErrorHandler.class);
            assertThat(context).hasSingleBean(SolaceConnectionManager.class);
            assertThat(context).doesNotHaveBean(jakarta.jms.ConnectionFactory.class);
        });
    }

    @Test
    void bindsDocumentedProperties() {
        contextRunner.withPropertyValues(CONNECTION)
                .withPropertyValues(
                        "numaan.solace.connection.reconnect-retries=-1",
                        "numaan.solace.connection.ssl.validate-certificate=false",
                        "numaan.solace.publisher.default-delivery-mode=NON_PERSISTENT",
                        "numaan.solace.publisher.send-timeout=5000",
                        "numaan.solace.consumer.concurrency=5",
                        "numaan.solace.consumer.acknowledge-mode=MANUAL",
                        "numaan.solace.consumer.retry.max-attempts=4",
                        "numaan.solace.consumer.retry.initial-interval=250ms")
                .run(context -> {
                    SolaceProperties properties = context.getBean(SolaceProperties.class);
                    assertThat(properties.getConnection().getReconnectRetries()).isEqualTo(-1);
                    assertThat(properties.getConnection().getSsl().isValidateCertificate()).isFalse();
                    assertThat(properties.getPublisher().getSendTimeout()).hasMillis(5000);
                    assertThat(properties.getPublisher().toSettings().defaultDeliveryMode().name())
                            .isEqualTo("NON_PERSISTENT");
                    assertThat(properties.getConsumer().toSettings().concurrency()).isEqualTo(5);
                    assertThat(properties.getConsumer().toSettings().acknowledgeMode().name()).isEqualTo("MANUAL");
                    assertThat(properties.getConsumer().toSettings().retry().maxAttempts()).isEqualTo(4);
                    assertThat(properties.getConsumer().toSettings().retry().initialInterval()).hasMillis(250);
                });
    }

    @Test
    void integrationCanBeDisabled() {
        contextRunner.withPropertyValues("numaan.solace.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(SolacePublisher.class);
            assertThat(context).doesNotHaveBean(SolaceConsumer.class);
            assertThat(context).doesNotHaveBean(SolaceConnectionManager.class);
        });
    }

    @Test
    void publisherOnlyApplication() {
        contextRunner.withPropertyValues(CONNECTION).withPropertyValues("numaan.solace.consumer.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(SolacePublisher.class);
                    assertThat(context).doesNotHaveBean(SolaceConsumer.class);
                });
    }

    @Test
    void consumerOnlyApplication() {
        contextRunner.withPropertyValues(CONNECTION).withPropertyValues("numaan.solace.publisher.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(SolaceConsumer.class);
                    assertThat(context).doesNotHaveBean(SolacePublisher.class);
                });
    }

    @Test
    void missingConnectionSettingsFailStartupWithAClearMessage() {
        contextRunner.withPropertyValues("numaan.solace.connection.host=tcp://localhost:55555").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure())
                    .hasRootCauseInstanceOf(SolaceConfigurationException.class)
                    .rootCause()
                    .hasMessageContaining("vpn-name");
        });
    }

    @Test
    void invalidRetrySettingsFailStartup() {
        contextRunner.withPropertyValues(CONNECTION)
                .withPropertyValues("numaan.solace.consumer.retry.max-attempts=0")
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseInstanceOf(IllegalArgumentException.class);
                });
    }

    @Test
    void applicationBeansReplaceTheDefaults() {
        contextRunner.withPropertyValues(CONNECTION).withUserConfiguration(CustomPublisherConfiguration.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(SolacePublisher.class);
                    assertThat(context).doesNotHaveBean(DefaultSolacePublisher.class);
                });
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomPublisherConfiguration {

        @Bean
        SolacePublisher customPublisher() {
            return new SolacePublisher() {
                @Override
                public PublishResult publish(String destination, Object payload, PublishOptions options) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public CompletableFuture<PublishResult> publishAsync(String destination, Object payload,
                        PublishOptions options) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
