package com.numaansystems.solace.examples.requestreply;

import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingRequest;
import com.numaansystems.solace.examples.requestreply.PricingMessages.PricingResponse;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Example E: request/response built entirely in the application on top of the
 * library's publisher and consumer. Requester and responder run in one
 * process here only to keep the example self-contained.
 */
@SpringBootApplication
public class RequestReplyExample {

    public static void main(String[] args) {
        SpringApplication.run(RequestReplyExample.class, args).close();
    }

    @Bean
    ApplicationRunner run(PricingClient client) {
        return args -> {
            for (int i = 0; i < 50 && !client.isReady(); i++) {
                Thread.sleep(100);
            }
            try {
                PricingResponse response = client.price(new PricingRequest("widget", 3), Duration.ofSeconds(5)).get();
                System.out.println("Price for 3 widgets: " + response.total());
            } catch (ExecutionException ex) {
                if (ex.getCause() instanceof TimeoutException) {
                    System.out.println("No response within 5 seconds");
                } else {
                    throw ex;
                }
            }
        };
    }
}
