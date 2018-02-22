package org.apache.ignite.failure;

/**
 * Failure Context.
 */
public class IgniteFailureContext {
    /** Type. */
    private final IgniteFailureType type;

    /** Cause. */
    private final Throwable cause;

    /**
     * @param type Type.
     * @param cause Cause.
     */
    public IgniteFailureContext(IgniteFailureType type, Throwable cause) {
        assert type != null;

        this.type = type;
        this.cause = cause;
    }

    /**
     * @return IgniteFailureType value.
     */
    public IgniteFailureType type() {
        return type;
    }

    /**
     * @return cause or {@code null}.
     */
    public Throwable cause() {
        return cause;
    }

    @Override public String toString() {
        return "IgniteFailureContext{" +
            "type=" + type +
            ", cause=" + cause +
            '}';
    }
}