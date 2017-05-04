package org.apache.ignite.internal.processors.cache.database.wal;

import org.apache.ignite.IgniteCheckedException;

/**
 * This exception is thrown either when we reach the end of file of WAL segment, or when we encounter
 * a record with type equal to {@code 0}.
 */
public class SegmentEofException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Message.
     * @param cause Cause.
     */
    public SegmentEofException(String msg, Throwable cause) {
        super(msg, cause, false);
    }
}
