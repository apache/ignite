/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.jetbrains.annotations.*;

/**
 * Exception thrown if NIO channel bind failed.
 */
public class GridNioBindException extends GridNioException {
    /**
     * @param msg Error message.
     */
    public GridNioBindException(String msg) {
        super(msg);
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     */
    public GridNioBindException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }

    /**
     * @param cause Cause.
     */
    public GridNioBindException(Throwable cause) {
        super(cause);
    }
}
