/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.security;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Common security exception for the grid.
 */
public class GridSecurityException extends GridRuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructs security grid exception with given message and cause.
     *
     * @param msg Exception message.
     * @param cause Exception cause.
     */
    public GridSecurityException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * Creates new security grid exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridSecurityException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Constructs security grid exception with given message.
     *
     * @param msg Exception message.
     */
    public GridSecurityException(String msg) {
        super(msg);
    }
}
