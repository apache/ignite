package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import org.apache.ignite.IgniteCheckedException;

/**
 * This exception is used in checking boundaries (StandaloneWalRecordsIterator).
 */
public class StrictBoundsCheckException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param mesg Message.
     */
    public StrictBoundsCheckException(String mesg) {
        super(mesg);
    }
}
