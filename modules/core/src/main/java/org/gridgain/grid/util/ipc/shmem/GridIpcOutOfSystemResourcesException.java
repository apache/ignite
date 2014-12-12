/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.apache.ignite.*;
import org.jetbrains.annotations.*;

/**
 * Thrown when IPC runs out of system resources (for example, no more free shared memory is
 * available in operating system).
 */
public class GridIpcOutOfSystemResourcesException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridIpcOutOfSystemResourcesException(String msg) {
        super(msg);
    }

    /**
     * Creates new exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridIpcOutOfSystemResourcesException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridIpcOutOfSystemResourcesException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
