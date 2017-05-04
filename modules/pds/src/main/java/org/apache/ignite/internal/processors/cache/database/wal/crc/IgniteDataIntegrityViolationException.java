package org.apache.ignite.internal.processors.cache.database.wal.crc;

import org.apache.ignite.IgniteException;

/**
 * Will be thrown if data integrity violation is found
 */
public class IgniteDataIntegrityViolationException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public IgniteDataIntegrityViolationException() {
    }

    /**
     * @param msg Message.
     */
    public IgniteDataIntegrityViolationException(String msg) {
        super(msg);
    }
}
