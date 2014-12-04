/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * This runtime exception can be thrown from {@link GridComputeJob#execute()} method to force
 * job failover to another node within task topology. Any
 * {@link org.apache.ignite.lang.IgniteClosure}, {@link Callable}, or {@link Runnable} instance passed into
 * any of the {@link org.apache.ignite.IgniteCompute} methods can also throw this exception to force failover.
 */
public class GridComputeJobFailoverException extends GridRuntimeException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     */
    public GridComputeJobFailoverException(String msg) {
        super(msg);
    }

    /**
     * Creates new given throwable as a cause and
     * source of error message.
     *
     * @param cause Non-null throwable cause.
     */
    public GridComputeJobFailoverException(Throwable cause) {
        this(cause.getMessage(), cause);
    }

    /**
     * Creates new exception with given error message
     * and optional nested exception.
     *
     * @param msg Error message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public GridComputeJobFailoverException(String msg, @Nullable Throwable cause) {
        super(msg, cause);
    }
}
