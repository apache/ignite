/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.gridgain.grid.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * This class defines a handler for asynchronous task execution. It's similar in design
 * to standard JDK {@link Future} interface but has improved and easier to use exception
 * hierarchy.
 * @param <R> Type of the task result returning from {@link GridComputeTask#reduce(List)} method.
 */
public interface GridComputeTaskFuture<R> extends GridFuture<R> {
    /**
     * {@inheritDoc}
     *
     * @throws GridComputeTaskTimeoutException If task execution timed out.
     */
    @Override public R get() throws GridException;

    /**
     * {@inheritDoc}
     *
     * @throws GridComputeTaskTimeoutException If task execution timed out.
     */
    @Override public R get(long timeout) throws GridException;

    /**
     * {@inheritDoc}
     *
     * @throws GridComputeTaskTimeoutException If task execution timed out.
     */
    @Override R get(long timeout, TimeUnit unit) throws GridException;

    /**
     * Gets task session of execution grid task.
     *
     * @return Task session.
     */
    public GridComputeTaskSession getTaskSession();

    /**
     * Checks if "<tt>map</tt>" step has completed (which means that {@link GridComputeTask#map(List, Object)}
     * method has finished).
     *
     * @return {@code true} if map step has completed.
     */
    public boolean isMapped();

    /**
     * Waits for a specified timeout in milliseconds for {@link GridComputeTask#map(List, Object)}
     * method to complete. This may be useful when it is desired to know the list of all
     * job siblings for the task, as list of job siblings gets finalized only after the map
     * step completes.
     * <p>
     * Note that this method will also return if the task fails.
     *
     * @param timeout Maximum time to wait, {@code 0} to wait forever.
     * @return {@code true} if map step has completed, {@code false} otherwise.
     * @throws GridException If got interrupted while waiting or any other failure.
     */
    public boolean waitForMap(long timeout) throws GridException;
}
