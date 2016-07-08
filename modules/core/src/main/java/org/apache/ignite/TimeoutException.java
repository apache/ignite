package org.apache.ignite;

/**
 * The exception thrown whenever time out take place.
 */
public class TimeoutException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new timeout exception with given error message.
     *
     * @param msg Error message.
     */
    public TimeoutException(String msg) {
        super(msg);
    }

    /**
     * Creates new timeout exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
