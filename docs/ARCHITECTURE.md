# Architecture and design decisions

Status: reviewed. Spring JMS, Spring Boot 4.x, the `numaan.solace` prefix, the direct-transport default and the topic concurrency limit are confirmed.

## 1. Repository assessment

The repository was empty when this work started: no existing Java, Spring Boot or Solace versions, no messaging conventions and no shared configuration starter to align with. Every version and naming choice below is therefore a proposal, not an alignment with existing code.

| Item | Choice | Why |
|---|---|---|
| Java | 17 (compiled with `--release 17`) | Minimum for Spring Boot 3 |
| Spring Boot | 4.1.1 | Current generation (Spring Framework 7, Jakarta EE 11). Spring Boot 3 is not supported |
| JSON | Jackson 3 (`tools.jackson`) | What Boot 4 auto-configures; the library uses the application's `JsonMapper` |
| Solace client | `sol-jms-jakarta` 10.30.0 | Solace's `jakarta.jms` client, required by Spring 6 and later |
| Coordinates | `com.numaansystems.solace:solace-messaging-*` | |
| Property prefix | `numaan.solace` | Avoids `solace.jms.*` / `solace.java.*` used by Solace's own starters |

## 2. JMS versus the native Solace API

**Decision: Spring JMS on top of the Solace JMS (Jakarta) client.**

Every required capability is available through JMS: topics and queues, persistent and non-persistent delivery, durable and non-durable subscriptions, client acknowledgement, local transactions, correlation ID, reply-to, TTL, priority and custom properties. Spring already provides the hard parts as maintained code:

- `DefaultMessageListenerContainer` gives concurrency, connection recovery with backoff, and a shutdown that waits for in-flight listeners.
- `CachingConnectionFactory` gives one shared, thread-safe connection with cached sessions and producers.
- `JmsTemplate` gives correct session and resource handling for sends.

The native API (JCSMP) would add per-message publish confirmations with correlation keys, negative acknowledgements with explicit outcomes, and direct-messaging features such as shared subscriptions. None is required by the brief, and each would mean writing and maintaining a custom listener and reconnection framework, which the brief rules out.

Solace-specific code is limited to three places in the `internal` package: building the connection factory, and two message property names (content type and DMQ eligibility). No Solace or JMS type appears in the public API.

Known cost of this choice: a blocking JMS send has no timeout of its own and publish confirmation is only available synchronously. Section 6 describes how the library handles that.

## 3. Modules

```
solace-messaging-parent
├── solace-messaging-core                  API + implementation (Spring JMS, Micrometer, Jackson)
├── solace-messaging-spring-boot-starter   properties, auto-configuration, health indicator
└── solace-messaging-examples              runnable examples A–E (not published)
```

The brief suggests separate `autoconfigure` and `starter` modules. They are merged: the split matters when the auto-configuration serves several optional starters, which is not the case here. Splitting later is backward compatible for consumers of the starter.

Packages:

- `com.numaansystems.solace` — the public, supported API.
- `com.numaansystems.solace.internal` — implementation. Public only so the starter (and non-Spring users) can construct it; no compatibility promise.
- `com.numaansystems.solace.autoconfigure` — Spring Boot wiring.

## 4. Public API

- `SolacePublisher` — `publish` (blocking, returns `PublishResult`) and `publishAsync` (`CompletableFuture`).
- `SolaceConsumer` — `register(options, [payloadType,] handler)` returning a `ConsumerRegistration` (`start`, `stop`, `isRunning`, `isReady`, `close`).
- `SolaceMessageHandler<T>`, `SolaceMessage<T>` (immutable), `PublishOptions`, `ConsumerOptions` (immutable, builders).
- Enums that keep the three concepts apart: `DestinationType` (TOPIC, QUEUE), `SubscriptionType` (DURABLE, NON_DURABLE), `DeliveryMode` (PERSISTENT, NON_PERSISTENT), plus `AcknowledgeMode` (AUTO, MANUAL, TRANSACTED).
- Extension points: `PayloadSerializer`, `SolaceErrorHandler`.
- Exceptions: `SolaceMessagingException` with `Connection`, `Publish`, `Consume`, `Serialization` and `Configuration` subclasses. The underlying JMS/Solace exception is always the cause.

Changes to the illustrative API in the brief:

- `publish` returns a `PublishResult` instead of `void`.
- The handler is generic (`SolaceMessageHandler<T>`) and may throw checked exceptions.
- Destination type is part of `PublishOptions` (default TOPIC) so the `publish(String, Object, PublishOptions)` shape is kept.
- `ConsumerOptions` is created from `queue(..)`, `durableTopic(..)` or `topic(..)`, which makes the invalid combinations (a durable queue consumer, a durable subscription without a name) impossible to express or rejected at `build()`.

There is deliberately no listener annotation and no request/response helper.

## 5. Connections, sessions and threads

- JMS connections are thread-safe; sessions, producers and consumers are not. The library never shares a session between threads.
- **Publishing:** one shared connection for the whole application, with up to 16 cached sessions. Sends run on a small pool of publisher threads (default 4).
- **Consuming:** each registration has its own listener container and its own connection, with one session per concurrent consumer. An application with N registrations therefore holds N + 1 broker connections.
- **Recovery, two layers:** the Solace client reconnects transparently (`reconnect-retries`, default 20 tries, 3 s apart). If it gives up, the shared connection is discarded and rebuilt on next use, and each listener container retries every `recovery-interval` (default 5 s) without limit.
- **Shutdown:** the consumer is a `SmartLifecycle` bean in a late phase, so it stops first and waits for in-flight handlers while the publisher is still usable (handlers often publish replies). The publisher then drains its queue for up to the send timeout.
- No client ID is set. Solace treats it as the client name, which must be unique per VPN, and the library opens several connections.

## 6. Delivery semantics

| Delivery mode | `direct-transport=false` (library default) | `direct-transport=true` |
|---|---|---|
| PERSISTENT | Guaranteed; blocking publish returns after broker acknowledgement | Same |
| NON_PERSISTENT | Guaranteed transport, not spooled to disk | Solace direct message: no acknowledgement, may be dropped |

The library default differs from the Solace client default (`true`) because direct transport rules out transacted sessions and silently changes what a non-durable topic consumer receives. Applications that want direct messaging opt in.

**Publish timeout and retries.** A blocking `publish` waits up to `send-timeout` for the publisher thread. On timeout it throws `SolacePublishException` with `isOutcomeUnknown() == true`; the send may still complete. Retries are off by default. When enabled, only failures that happened before the message was handed to the client (for example, no connection) are retried. Failures during the send have an unknown outcome and are retried only if `retry-on-unknown-outcome=true`, which can duplicate messages. Exactly-once is never claimed.

**Subscriptions.**

| Destination | Subscription | Maps to | Concurrency |
|---|---|---|---|
| QUEUE | n/a | Consumer flow bound to the queue | Any; needs a non-exclusive queue to spread load |
| TOPIC | DURABLE | Durable topic endpoint named by the subscription name | 1 only |
| TOPIC | NON_DURABLE | Temporary endpoint (or direct subscription with direct transport) | 1 only |

Topic consumers are limited to concurrency 1: several non-durable consumers would each receive every message, and a durable topic endpoint takes a single active subscriber. The supported way to scale topic consumption on Solace is to add the topic subscription to a queue on the broker and consume the queue.

## 7. Acknowledgement, retry and dead-lettering

The listener container is configured so that it never acknowledges by itself; one class (`MessageProcessor`) makes every settle decision.

| Mode | Handler succeeds | Handler fails after all retries |
|---|---|---|
| AUTO | acknowledge | dead-letter and acknowledge, or `session.recover()` |
| MANUAL | acknowledged by handler; if not, `session.recover()` | same as AUTO |
| TRANSACTED | container commits | dead-letter and commit, or rollback |

- A message is never acknowledged before the handler has succeeded.
- **Retry** is in-process with exponential backoff and blocks that consumer thread. Undecodable payloads skip retries.
- **Dead-letter queue selection:** the queue set on `ConsumerOptions`, else none if the consumer called `noDeadLetterQueue()`, else `consumer.dead-letter-queue` from configuration. The configured value may contain `{destination}` (queue name, or durable subscription name) so one property yields a queue per consumer. A consumer of the configured queue itself is excluded to prevent a forwarding loop.
- **Dead-lettering**, when a queue applies, republishes the original message to that queue on the consumer's own session. In TRANSACTED mode this is atomic with consuming the original. In AUTO/MANUAL a crash between the send and the acknowledgement leaves the message in both queues.
- Without a dead-letter queue the message goes back to the broker. **The broker's max-redelivery setting on the endpoint is then the only thing that ends the loop**; with max-redelivery 0 (unlimited) a poison message is redelivered forever, slowed only by the retry backoff. Published messages are DMQ-eligible by default so the broker can move them to its dead message queue.
- Duplicates are possible after a failed acknowledgement, a rollback, a reconnect, or an unknown-outcome publish retry. Handlers should be idempotent; `SolaceMessage.getMessageId()` is stable across redeliveries.

## 8. Message metadata

| Field | Carried as | Set by |
|---|---|---|
| Message ID (application) | property `app_message_id` | publisher (UUID if not supplied) |
| Provider message ID | `JMSMessageID` | Solace client |
| Correlation ID | `JMSCorrelationID` | application |
| Reply-To | `JMSReplyTo` | application |
| Content type | `JMS_Solace_HTTPContentType` | serializer or application |
| Expiration | `JMSExpiration` (from TTL) | client |
| Priority | `JMSPriority` | application |
| Timestamp | `JMSTimestamp` | client |
| Redelivered / delivery count | `JMSRedelivered` / `JMSXDeliveryCount` | broker |
| Custom properties | JMS properties | application |

The application message ID is a separate property because the JMS message ID is assigned by the client on send and cannot be chosen by the application. Custom property names are validated against the JMS rules (identifier syntax, no `JMS` prefix, no selector keywords) and values against the JMS property types, at build time rather than at send time.

Payloads: `String` → text message, `byte[]` → bytes message, anything else → JSON text via Jackson. Map, stream and object messages are rejected on receive.

## 9. Configuration model

`numaan.solace.{enabled, connection.*, publisher.*, consumer.*}`; see the README reference. Differences from the example in the brief:

- Retry is configured separately under `publisher.retry` and `consumer.retry`, because retrying a publish and retrying a handler have different safety properties and different sensible defaults (1 and 3 attempts).
- Connection settings are validated at startup; a missing host, VPN or credential fails the context with a message naming the property.
- No secrets, hosts or destination names are in the library. Vault integration is whatever the application already uses to populate Spring properties.

## 10. Limitations and open questions for review

1. **Spring Boot 4 specifics.** The health indicator implements `org.springframework.boot.health.contributor.HealthIndicator` and is switched by `management.health.solace.enabled` only; it does not follow `management.health.defaults.enabled`. Jackson 2 is not supported.
2. **Send timeout** is enforced by the calling side, not by the client; a timed-out send is not cancelled.
3. **Shared durable subscriptions** (several consumers on one durable subscription) are not offered; use a queue with a topic subscription.
4. **Connections:** one per consumer registration. Fine for typical services; revisit if a service registers dozens of consumers.
5. **Health** reports DOWN only after the Solace client has exhausted its reconnect attempts; during transparent reconnection it stays UP.
6. **Dead-letter republish** forwards the message unchanged; it does not add failure-reason properties.
7. **No XA.** TRANSACTED means a local JMS transaction.
8. **Verification status:** see the README section "Build and verification status".
