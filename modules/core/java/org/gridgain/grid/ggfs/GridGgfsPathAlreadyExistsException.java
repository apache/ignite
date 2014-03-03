/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.jetbrains.annotations.*;

/**
 * Exception thrown when target path supposed to be created already exists.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsPathAlreadyExistsException extends GridGgfsInvalidPathException {
    /**
     * @param msg Error message.
     */
    public GridGgfsPathAlreadyExistsException(String msg) {
        super(msg);
    }

    /**
     * @param cause Exception cause.
     */
    public GridGgfsPathAlreadyExistsException(Throwable cause) {
        super(cause);
    }

    /**
     * @param msg Error message.
     * @param cause Exception cause.
     */
    public GridGgfsPathAlreadyExistsException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
