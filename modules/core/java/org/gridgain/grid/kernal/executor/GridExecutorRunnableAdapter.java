/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.executor;

import java.io.*;
import java.util.concurrent.*;

/**
 * Wraps {@link Runnable} task with predefined result.
 * @param <T> The result type of the {@link Callable} argument.
 */
class GridExecutorRunnableAdapter<T> implements Callable<T>, Serializable {
    /** Returned result. */
    private final T result;

    /** Wrapped task. */
    private final Runnable task;

    /**
     * Creates adapter for runnable command.
     *
     * @param task The runnable task.
     * @param result Returned result.
     */
    GridExecutorRunnableAdapter(Runnable  task, T result) {
        this.task = task;
        this.result = result;
    }

    /** {@inheritDoc} */
    @Override public T call() {
        task.run();

        return result;
    }
}
