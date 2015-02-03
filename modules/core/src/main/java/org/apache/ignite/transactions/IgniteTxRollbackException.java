/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.transactions;

import org.apache.ignite.*;

/**
 * Exception thrown whenever grid transactions has been automatically rolled back.
 */
public class IgniteTxRollbackException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new rollback exception with given error message.
     *
     * @param msg Error message.
     */
    public IgniteTxRollbackException(String msg) {
        super(msg);
    }

    /**
     * Creates new rollback exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be <tt>null</tt>).
     */
    public IgniteTxRollbackException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
