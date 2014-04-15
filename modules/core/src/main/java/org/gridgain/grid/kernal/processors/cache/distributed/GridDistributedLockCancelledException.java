/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

/**
 * Exception thrown whenever an attempt is made to acquire a cancelled lock.
 */
public class GridDistributedLockCancelledException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;


    /**
     *
     */
    public GridDistributedLockCancelledException() {
        // No-op.
    }

    /**
     * @param msg Message.
     */
    public GridDistributedLockCancelledException(String msg) {
        super(msg);
    }
}