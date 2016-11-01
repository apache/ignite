package org.apache.ignite.internal.processors.rest.handlers.redis.exception;

import org.apache.ignite.IgniteCheckedException;

/**
 * Generic Redis protocol exception.
 */
public class GridRedisGenericException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a generic exception with given error message.
     *
     * @param msg Error message.
     */
    public GridRedisGenericException(String msg) {
        super(msg);
    }
}
