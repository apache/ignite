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
 * Thrown when IPC operation (such as {@link GridIpcSharedMemorySpace#wait(long)})
 * has timed out.
 */
public class GridIpcSharedMemoryOperationTimedoutException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridIpcSharedMemoryOperationTimedoutException(String msg) {
        super(msg);
    }

    /**
     * Creates new exception with given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridIpcSharedMemoryOperationTimedoutException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates new exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridIpcSharedMemoryOperationTimedoutException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
