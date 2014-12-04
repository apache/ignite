/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.compute;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * This exception indicates that task execution timed out. It is thrown from
 * {@link ComputeTaskFuture#get()} method.
 */
public class GridComputeTaskTimeoutException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates task timeout exception with given task execution ID and
     * error message.
     *
     * @param msg Error message.
     */
    public GridComputeTaskTimeoutException(String msg) {
        super(msg);
    }

    /**
     * Creates new task timeout exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridComputeTaskTimeoutException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates task timeout exception with given task execution ID,
     * error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridComputeTaskTimeoutException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
