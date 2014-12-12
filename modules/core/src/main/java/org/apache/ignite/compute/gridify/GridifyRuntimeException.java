/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute.gridify;

import org.apache.ignite.*;

/**
 * This defines gridify exception. This runtime exception gets thrown out of gridified
 * methods in case if method execution resulted in undeclared exception.
 */
public class GridifyRuntimeException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new gridify runtime exception with specified message.
     *
     * @param msg Exception message.
     */
    public GridifyRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Creates new gridify runtime exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridifyRuntimeException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new gridify runtime exception with specified message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public GridifyRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
