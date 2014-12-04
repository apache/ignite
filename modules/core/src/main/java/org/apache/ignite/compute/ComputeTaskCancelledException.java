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
 * This exception indicates that grid task was cancelled.
 */
public class ComputeTaskCancelledException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new task cancellation exception with given error message.
     *
     * @param msg Error message.
     */
    public ComputeTaskCancelledException(String msg) {
        super(msg);
    }

    /**
     * Creates new task cancellation exception given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public ComputeTaskCancelledException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new task cancellation exception with given error message and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public ComputeTaskCancelledException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
