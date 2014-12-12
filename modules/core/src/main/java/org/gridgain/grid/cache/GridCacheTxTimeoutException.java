/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache;

import org.apache.ignite.*;

/**
 * Exception thrown whenever grid transactions time out.
 */
public class GridCacheTxTimeoutException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new timeout exception with given error message.
     *
     * @param msg Error message.
     */
    public GridCacheTxTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Creates new timeout exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be <tt>null</tt>).
     */
    public GridCacheTxTimeoutException(String msg, Throwable cause) {
        super(msg, cause);
    }
}