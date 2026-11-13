# Solace Messaging Library

A shared Spring Boot starter for Solace PubSub+. It gives a service a publisher, a consumer, a common message model, connection management, health and metrics. It is messaging infrastructure only: business workflows, including request/response orchestration, stay in the applications.

Design rationale is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Build and verification status

**This code has not been compiled or run yet.** It was written in an environment without access to Maven repositories, so no dependency could be downloaded. Only the dependency-free API classes were compiled (with plain `javac`). Before relying on it:

```bash
mvn verify                        # compile + unit tests
mvn verify -Pintegration-test     # plus broker tests in Docker (Testcontainers)
```

Expect to fix small compile or test errors on the first run. Connection recovery after a broker restart is not covered by an automated test.

## Modules and dependency

| Module | Purpose |
|---|---|
| `solace-messaging-core` | API and implementation |
| `solace-messaging-spring-boot-starter` | Auto-configuration; the only dependency a service needs |
| `solace-messaging-examples` | Runnable examples, not published |

```xml
<dependency>
    <groupId>com.numaansystems.solace</groupId>
    <artifactId>solace-messaging-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Supported versions: Java 17+, Spring Boot 4.1.x (Spring Framework 7, Jackson 3), Solace JMS (Jakarta) client 10.30.x. Spring Boot 3 is not supported. Add `spring-boot-starter-actuator` (and `spring-boot-health`, if your Actuator setup does not already bring it) to get the health indicator and metrics export.

## Quick start

```yaml
numaan:
  solace:
    connection:
      host: ${SOLACE_HOST}          # e.g. tcps://broker.example.com:55443
      vpn-name: ${SOLACE_VPN}
      username: ${SOLACE_USERNAME}
      password: ${SOLACE_PASSWORD}
```

```java
@Service
class Orders {

    Orders(SolacePublisher publisher, SolaceConsumer consumer) {
        consumer.register(ConsumerOptions.queue("orders.inbound").concurrency(4).build(),
                OrderCreated.class,
                message -> handle(message.getPayload()));
    }
}
```

No connection factory or JMS bean needs to be declared.

## Publisher API

```java
PublishResult result = solacePublisher.publish(
        "pricing/request",
        pricingRequest,
        PublishOptions.builder()
                .deliveryMode(DeliveryMode.PERSISTENT)
                .correlationId("REQ-123456")
                .replyTo("pricing/response/clientA")
                .build());
```

- Payloads: `String` (text), `byte[]` (binary), any other object (JSON via Jackson 3, using the application's `JsonMapper` bean when there is one). Replace the `PayloadSerializer` bean to change this.
- `PublishOptions`: destination type (topic by default, or queue), delivery mode, correlation ID, reply-to (topic or queue), message ID, content type, time-to-live, priority, send timeout, DMQ eligibility, custom properties.
- `publish` blocks; `publishAsync` returns a `CompletableFuture`. Asynchronous publishes are not ordered relative to each other.
- A persistent publish returns after the broker has acknowledged the message. A non-persistent publish returns once the client has taken the message.
- Failures throw `SolacePublishException`. `isOutcomeUnknown()` tells you whether the message may have reached the broker anyway (timeout, or failure mid-send). Republishing in that case can create a duplicate.
- Retries are off by default. `publisher.retry.max-attempts` retries failures where nothing was sent; `publisher.retry-on-unknown-outcome=true` also retries unknown outcomes and accepts duplicates.

Solace topics are hierarchical with `/` separators (`orders/created/eu`); wildcards `*` and `>` work only on those levels. Dots are ordinary characters.

## Consumer API

```java
ConsumerRegistration registration = solaceConsumer.register(
        ConsumerOptions.queue("orders.inbound")
                .concurrency(4)
                .acknowledgeMode(AcknowledgeMode.AUTO)
                .retryPolicy(new RetryPolicy(3, Duration.ofSeconds(1), 2.0, Duration.ofSeconds(10)))
                .deadLetterQueue("orders.inbound.dlq")
                .build(),
        OrderCreated.class,
        message -> process(message.getPayload()));

registration.stop();    // pause delivery
registration.start();   // resume
registration.close();   // deregister; waits for the in-flight handler
```

| Factory | Meaning |
|---|---|
| `ConsumerOptions.queue(name)` | Consume a queue |
| `ConsumerOptions.durableTopic(topic, subscriptionName)` | Durable topic subscription |
| `ConsumerOptions.topic(topic)` | Non-durable topic subscription |

Registrations made before the application context has started (in a constructor or `@PostConstruct`) begin delivery when the context is ready. `isReady()` reports when the subscription is actually attached, which matters for non-durable topics.

Invalid or unsupported combinations fail with `SolaceConfigurationException`: a durable subscription without a name, topic concurrency above 1, transacted consumption with direct transport, duplicate registration names.

## Message metadata

`SolaceMessage<T>` is immutable and exposes:

| Metadata | Accessor | Managed by |
|---|---|---|
| Message-ID | `getMessageId()` (application ID, stable across redelivery), `getProviderMessageId()` | publisher / client |
| Correlation-ID | `getCorrelationId()` | application |
| Reply-To | `getReplyTo()`, `getReplyToType()` | application |
| Content-Type | `getContentType()` | serializer or application |
| Expiration | `getExpiration()` | client, from TTL |
| Priority | `getPriority()` | application |
| Timestamp | `getTimestamp()` | client |
| Redelivery | `isRedelivered()`, `getDeliveryCount()` | broker |
| Custom properties | `getProperties()` | application |

Metadata travels in message headers and properties, never inside the payload. Custom property names must be valid identifiers (letters, digits, `_`, `$`), must not start with `JMS`, and values must be a `String`, `Boolean` or numeric wrapper; violations are rejected when the options are built.

## Semantics

Three independent choices:

**Destination type — queue or topic.** A queue holds each message until one consumer acknowledges it; several consumers on a queue compete. A topic delivers a copy to every matching subscription.

**Delivery mode — persistent or non-persistent.** Set by the publisher per message.

- PERSISTENT: the broker stores the message and acknowledges the publisher. It survives broker restarts and waits on queues and durable endpoints. It is still lost if nothing is subscribed to the topic, if the endpoint's spool quota is exceeded, or when its TTL passes.
- NON_PERSISTENT: not written to disk; can be lost on broker failover. With `connection.direct-transport=true` it becomes a Solace direct message: lowest latency, no acknowledgement, dropped if the consumer is slow or absent.

**Subscription type — durable or non-durable.** Applies to topics only, and is set by the consumer.

- DURABLE: the broker keeps a durable topic endpoint named after the subscription name. Persistent messages published while the consumer is offline wait there. The endpoint lives until an administrator deletes it and keeps accumulating messages while nobody consumes.
- NON_DURABLE: exists only while the consumer is connected. Messages published while it is offline are not delivered to it, whatever their delivery mode.

A durable subscription does not make messages persistent, and a persistent message is not retained for a non-durable subscriber that is offline.

**Broker provisioning.** Queues and durable topic endpoints must exist on the broker, with the consuming client's permission to bind and, for durable topics, the subscription. `connection.dynamic-durables=true` lets the client create them and is meant for development and tests. Spreading load across competing consumers needs a non-exclusive queue; on an exclusive queue only one consumer is active and the others stand by. TTL and priority take effect only on endpoints with "respect TTL" / "respect message priority" enabled. To consume a topic with more than one consumer, add the topic subscription to a queue and consume the queue.

## Acknowledgement, retry, redelivery, dead letters

| Mode | When the message is acknowledged |
|---|---|
| `AUTO` (default) | By the library, after the handler returns normally |
| `MANUAL` | By the handler, calling `message.acknowledge()` on the handler thread before returning. Returning without it requests redelivery |
| `TRANSACTED` | Local JMS transaction committed after the handler returns; rolled back if it fails |

When a handler throws:

1. It is retried in-process per the retry policy (default 3 attempts, 1 s then 2 s). The consumer thread is blocked meanwhile.
2. After the last attempt the `SolaceErrorHandler` bean is called (default: log without payload).
3. If a dead-letter queue applies (see below), the message is republished there and removed from the source.
4. Otherwise it is released back to the broker and redelivered with `isRedelivered() == true`.

Step 4 repeats until the endpoint's **max-redelivery** count is reached, after which the broker discards the message or moves it to its dead message queue (messages are published DMQ-eligible by default). If max-redelivery is 0 (unlimited) and no dead-letter queue applies, a message that always fails is redelivered forever. Configure one of the two.

**Choosing the dead-letter queue.** In order of precedence:

1. `ConsumerOptions...deadLetterQueue("orders.dlq")` on the registration.
2. `ConsumerOptions...noDeadLetterQueue()` opts that consumer out.
3. `numaan.solace.consumer.dead-letter-queue` in configuration, applied to every other consumer:

```yaml
numaan:
  solace:
    consumer:
      dead-letter-queue: "{destination}.dlq"   # or one fixed queue name for all consumers
```

`{destination}` becomes the consumed queue name, or the subscription name for a durable topic subscription. A pattern with the placeholder is not applied to non-durable topic subscriptions, which have no stable name. A consumer reading the configured queue itself is never forwarded to it. The dead-letter queues must exist on the broker; if the republish fails, the message is released for redelivery instead.

Payloads that cannot be deserialized skip step 1.

All modes are at-least-once. A message can arrive twice after a failed acknowledgement, a rollback, a reconnect, or a dead-letter republish interrupted by a crash. Make handlers idempotent, keyed on `getMessageId()`.

## Request/response

The library supplies correlation ID, reply-to, and the publisher and consumer. The application owns pending-request state, the reply listener, matching, timeouts and duplicates.

Responder:

```java
void process(SolaceMessage<PricingRequest> message) {
    PricingResponse response = calculate(message.getPayload());
    solacePublisher.publish(message.getReplyTo(), response, PublishOptions.builder()
            .destinationType(message.getReplyToType())
            .correlationId(message.getCorrelationId())
            .build());
}
```

A complete requester with timeout handling is in [`examples/requestreply`](solace-messaging-examples/src/main/java/com/numaansystems/solace/examples/requestreply).

A reply-to topic does not guarantee the reply arrives. With a non-durable reply subscription, a reply sent while the requester is disconnected, or before its subscription is active, is lost. If replies must survive that, give the requester a provisioned queue or durable subscription as its reply destination.

## Connection recovery

- The Solace client reconnects on its own: `reconnect-retries` attempts (default 20), `reconnect-retry-wait` apart (default 3 s). Publishes block during that window and may time out.
- If the client gives up, the shared publishing connection is rebuilt on the next publish, and each consumer retries every `consumer.recovery-interval` (default 5 s) indefinitely.
- A comma-separated `host` list enables failover between brokers.
- Unacknowledged messages are redelivered after a reconnect.

## Configuration reference

Prefix `numaan.solace`. Durations accept `500ms`, `5s`, or a plain number of milliseconds.

| Property | Default | Description |
|---|---|---|
| `enabled` | `true` | Turn the whole integration off |
| `connection.host` | — | Broker URL(s); `tcps://` for TLS |
| `connection.vpn-name` | — | Message VPN |
| `connection.username` / `password` | — | Required for `BASIC` authentication |
| `connection.authentication-scheme` | `BASIC` | `BASIC` or `CLIENT_CERTIFICATE` |
| `connection.direct-transport` | `false` | Send NON_PERSISTENT as direct messages |
| `connection.dynamic-durables` | `false` | Let the client create queues and durable endpoints |
| `connection.connect-timeout` | `30s` | One connection attempt |
| `connection.read-timeout` | `10s` | Broker reply timeout |
| `connection.keep-alive-interval` | `3s` | Keep-alive period |
| `connection.connect-retries` | `3` | Initial connection retries; `-1` forever |
| `connection.connect-retries-per-host` | `0` | Retries per host before trying the next |
| `connection.reconnect-retries` | `20` | Transparent reconnect attempts; `-1` forever |
| `connection.reconnect-retry-wait` | `3s` | Wait between attempts |
| `connection.compression-level` | `0` | ZLIB level 0–9 |
| `connection.ssl.trust-store` / `-password` / `-format` | JVM default | Trust store |
| `connection.ssl.key-store` / `-password` / `-format` | — | Client certificate |
| `connection.ssl.validate-certificate` | `true` | Verify the broker certificate |
| `connection.ssl.validate-certificate-date` | `true` | Verify certificate dates |
| `publisher.enabled` | `true` | Register `SolacePublisher` |
| `publisher.default-delivery-mode` | `PERSISTENT` | Used when a publish does not say |
| `publisher.send-timeout` | `5s` | Blocking publish wait |
| `publisher.dmq-eligible` | `true` | Default DMQ eligibility |
| `publisher.threads` | `4` | Sending threads |
| `publisher.queue-capacity` | `1000` | Waiting publishes before rejection |
| `publisher.retry.max-attempts` | `1` | Total attempts; 1 = no retry |
| `publisher.retry.initial-interval` / `multiplier` / `max-interval` | `1s` / `2` / `10s` | Backoff |
| `publisher.retry-on-unknown-outcome` | `false` | Also retry when the message may have been sent |
| `consumer.enabled` | `true` | Register `SolaceConsumer` |
| `consumer.concurrency` | `1` | Default for queue consumers |
| `consumer.acknowledge-mode` | `AUTO` | `AUTO`, `MANUAL`, `TRANSACTED` |
| `consumer.receive-timeout` | `1s` | Poll timeout of consumer threads |
| `consumer.recovery-interval` | `5s` | Wait between consumer reconnect attempts |
| `consumer.dead-letter-queue` | — | Default dead-letter queue; may contain `{destination}` |
| `consumer.retry.max-attempts` | `3` | Handler attempts |
| `consumer.retry.initial-interval` / `multiplier` / `max-interval` | `1s` / `2` / `10s` | Backoff |

Publisher-only service: `consumer.enabled=false`. Consumer-only service: `publisher.enabled=false`.

Every bean is `@ConditionalOnMissingBean`. Declare your own `PayloadSerializer`, `SolaceErrorHandler`, `SolaceMetrics`, `SolaceConnectionManager`, `SolacePublisher` or `SolaceConsumer` to replace the default.

### Secrets and Vault

The library contains no hosts, VPN names, credentials or certificates. It reads ordinary Spring properties, so any property source works. With Spring Cloud Vault:

```yaml
spring:
  config:
    import: vault://secret/my-service/solace
numaan:
  solace:
    connection:
      username: ${solace.username}
      password: ${solace.password}
```

Secrets stored under the keys `numaan.solace.connection.username` and `numaan.solace.connection.password` bind with no mapping at all. Credentials are read once at startup; rotating them requires a restart (or a context refresh). Passwords are never logged and are excluded from `toString()`.

## Metrics and health

Micrometer meters (registered when a `MeterRegistry` bean exists):

| Meter | Type | Tags |
|---|---|---|
| `solace.publish` | timer | `destination.type`, `delivery.mode`, `outcome` |
| `solace.publish.retries` | counter | — |
| `solace.consumer.received` | counter | `consumer`, `redelivered` |
| `solace.consumer.processing` | timer | `consumer`, `outcome` (`success`, `failure`, `dead_lettered`) |
| `solace.consumer.retries` | counter | `consumer` |
| `solace.consumer.active` | gauge | `consumer` |

Destination names are not used as tags, to keep cardinality bounded.

With Actuator and `spring-boot-health` on the classpath, `/actuator/health` includes a `solace` component (disable with `management.health.solace.enabled=false`). It is DOWN when a broker connection cannot be established. While the client is reconnecting transparently it stays UP.

Logging: while a handler runs, the MDC holds `solace.consumer`, `solace.messageId` and `solace.correlationId`. Payloads are never logged.

## Examples

Start a local broker (see `solace-messaging-examples/src/main/resources/application.yml`), then:

```bash
mvn -q install -DskipTests
cd solace-messaging-examples
mvn spring-boot:run -Dexample.main=com.numaansystems.solace.examples.queueconsumer.QueueConsumerExample
```

| Example | Main class (under `...solace.examples`) |
|---|---|
| A — publisher, persistent JSON | `publisher.PublisherExample` |
| B — queue consumer | `queueconsumer.QueueConsumerExample` |
| C — durable topic subscriber | `durabletopic.DurableTopicSubscriberExample` |
| D — non-durable topic subscriber | `nondurabletopic.NonDurableTopicSubscriberExample` |
| E — request/response | `requestreply.RequestReplyExample` |

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Startup fails: `Solace connection setting 'x' is required` | Property missing or the Vault/env placeholder did not resolve |
| `SolacePublishException`, cause mentions queue or endpoint not found | Queue not provisioned, or no permission to publish to it |
| Durable subscriber fails to bind | Durable topic endpoint not provisioned, already bound by another client, or bound with a different topic |
| Topic consumer misses messages | Non-durable subscription was not active yet (`isReady()`), or publisher uses direct transport |
| Only one of several queue consumers gets messages | Queue access type is exclusive |
| Same message arrives over and over | Handler keeps failing and the endpoint has no max-redelivery; set it or configure `consumer.dead-letter-queue` |
| Publishes time out during broker maintenance | Client is reconnecting; raise `publisher.send-timeout` or handle `isOutcomeUnknown()` |
| `Client Name Already In Use` | Something sets a fixed JMS client ID; the library does not |

## Operations

- Each service holds one connection for publishing plus one per consumer registration; size the broker's connection limits accordingly.
- Monitor spool usage of durable endpoints whose consumers are stopped; they keep accumulating.
- Alert on `solace.consumer.processing{outcome="failure"}` and on dead-letter queue depth.
- On shutdown the service stops consuming first, lets in-flight handlers finish, then closes the publisher. Give the pod a termination grace period longer than the slowest handler plus its retry backoff.

## Releasing

```bash
mvn deploy -Prelease -Dinternal.releases.url=... -Dinternal.snapshots.url=...
```

Repository IDs default to `internal-releases` / `internal-snapshots`; put matching credentials in `settings.xml`. Versioning is semantic: only the `com.numaansystems.solace` package (not `.internal`) and the `numaan.solace.*` properties are covered by compatibility guarantees.
