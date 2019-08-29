package de.bwaldvogel.mongo.backend;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

public class Assert {

    public static void isEmpty(Collection<?> values) {
        isEmpty(values, () -> "Expected " + values + " to be empty");
    }

    public static void isEmpty(Collection<?> values, Supplier<String> messageSupplier) {
        if (!values.isEmpty()) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void notEmpty(Collection<?> values) {
        notEmpty(values, () -> "Given collection must not be empty");
    }

    public static void hasSize(Collection<?> values, int expectedSize) {
        hasSize(values, expectedSize,
            () -> "Expected " + values + " to have size " + expectedSize + " but got " + values.size() + " elements");
    }

    public static void hasSize(Collection<?> values, int expectedSize, Supplier<String> messageSupplier) {
        if (values.size() != expectedSize) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void notEmpty(Collection<?> values, Supplier<String> messageSupplier) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static <T> void equals(T one, T other) {
        equals(one, other, () -> "Expected '" + one + "' to be equal to '" + other + "'");
    }

    public static void equals(long one, long other) {
        equals(one, other, () -> "Expected " + one + " to be equal to " + other);
    }

    public static <T> void equals(T one, T other, Supplier<String> messageSupplier) {
        if (!Objects.equals(one, other)) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void equals(long one, long other, Supplier<String> messageSupplier) {
        if (one != other) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void isTrue(boolean value, Supplier<String> messageSupplier) {
        if (!value) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void isFalse(boolean value, Supplier<String> messageSupplier) {
        if (value) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void isNull(Object value) {
        isNull(value, () -> "Given value '" + value + "' must not be null");
    }

    public static void isNull(Object value, Supplier<String> messageSupplier) {
        if (value != null) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void notNull(Object value) {
        notNull(value, () -> "Given value must not be null");
    }

    public static void notNull(Object value, Supplier<String> messageSupplier) {
        if (value == null) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void startsWith(String string, String requiredPrefix) {
        startsWith(string, requiredPrefix, () -> "'" + string + "' must start with '" + requiredPrefix + "'");
    }

    public static void startsWith(String string, String requiredPrefix, Supplier<String> messageSupplier) {
        if (!string.startsWith(requiredPrefix)) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void doesNotStartWith(String string, String forbiddenPrefix) {
        doesNotStartWith(string, forbiddenPrefix, () -> "'" + string + "' must not start with '" + forbiddenPrefix + "'");
    }

    public static void doesNotStartWith(String string, String forbiddenPrefix, Supplier<String> messageSupplier) {
        if (string.startsWith(forbiddenPrefix)) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }

    public static void notNullOrEmpty(String string) {
        notNullOrEmpty(string, () -> "Given string '" + string + "' must not be null or empty");
    }

    public static void notNullOrEmpty(String string, Supplier<String> messageSupplier) {
        if (string == null || string.isEmpty()) {
            throw new IllegalArgumentException(messageSupplier.get());
        }
    }
}
