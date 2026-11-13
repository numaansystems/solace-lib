package com.numaansystems.solace.examples.requestreply;

import java.math.BigDecimal;

/** Payloads for the request/response example. */
public final class PricingMessages {

    private PricingMessages() {
    }

    public record PricingRequest(String product, int quantity) {
    }

    public record PricingResponse(String product, BigDecimal total) {
    }
}
