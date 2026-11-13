package com.numaansystems.solace;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Validation rules for application-defined message properties. These follow
 * the JMS rules, which the Solace client enforces on the wire.
 */
final class MessageProperties {

    private static final Set<String> RESERVED_WORDS = Set.of(
            "NULL", "TRUE", "FALSE", "NOT", "AND", "OR", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE");

    private MessageProperties() {
    }

    static Map<String, Object> validatedCopy(Map<String, ?> properties) {
        Map<String, Object> copy = new LinkedHashMap<>();
        properties.forEach((name, value) -> {
            validate(name, value);
            copy.put(name, value);
        });
        return Collections.unmodifiableMap(copy);
    }

    static void validate(String name, Object value) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property name must not be empty");
        }
        if (!Character.isJavaIdentifierStart(name.charAt(0))) {
            throw new IllegalArgumentException("Property name '" + name + "' must start with a letter, '_' or '$'");
        }
        for (int i = 1; i < name.length(); i++) {
            if (!Character.isJavaIdentifierPart(name.charAt(i))) {
                throw new IllegalArgumentException(
                        "Property name '" + name + "' may only contain letters, digits, '_' and '$'");
            }
        }
        if (name.startsWith("JMS")) {
            throw new IllegalArgumentException(
                    "Property name '" + name + "' uses the 'JMS' prefix, which is reserved for the broker client");
        }
        if (RESERVED_WORDS.contains(name.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Property name '" + name + "' is a reserved word");
        }
        if (SolaceMessage.APPLICATION_MESSAGE_ID_PROPERTY.equals(name)) {
            throw new IllegalArgumentException(
                    "Property name '" + name + "' is reserved by the library; use messageId(...) instead");
        }
        if (!(value instanceof String || value instanceof Boolean || value instanceof Byte
                || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof Float || value instanceof Double)) {
            throw new IllegalArgumentException("Property '" + name + "' has unsupported type "
                    + (value == null ? "null" : value.getClass().getName())
                    + "; allowed: String, Boolean, Byte, Short, Integer, Long, Float, Double");
        }
    }
}
