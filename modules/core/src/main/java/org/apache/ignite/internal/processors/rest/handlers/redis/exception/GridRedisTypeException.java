package org.apache.ignite.internal.processors.rest.handlers.redis.exception;

import org.apache.ignite.IgniteCheckedException;

/**
 * Exception on operation on the wrong data type.
 */
public class GridRedisTypeException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a type exception with given error message.
     *
     * @param msg Error message.
     */
    public GridRedisTypeException(String msg) {
        super(msg);
    }
}
