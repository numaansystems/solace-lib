package com.numaansystems.solace.internal;

import com.numaansystems.solace.SolaceErrorHandler;
import com.numaansystems.solace.SolaceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default error handler: logs the failure without the payload. */
public class LoggingSolaceErrorHandler implements SolaceErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingSolaceErrorHandler.class);

    @Override
    public void handle(String consumerName, SolaceMessage<?> message, Throwable error) {
        log.error("Message processing failed consumer={} messageId={} correlationId={} destination={} redelivered={}",
                consumerName, message.getMessageId(), message.getCorrelationId(), message.getDestination(),
                message.isRedelivered(), error);
    }
}
