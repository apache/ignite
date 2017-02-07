package org.apache.ignite.stream.zeromq;

public class ZeroMqStreamerException extends Exception {
    /**
     * Create empty exception.
     */
    public ZeroMqStreamerException() {
        // No-op.
    }

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public ZeroMqStreamerException(String msg) {
        super(msg);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass() + ": " + getMessage();
    }
}
