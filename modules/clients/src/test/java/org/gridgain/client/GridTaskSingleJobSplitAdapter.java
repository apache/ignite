/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Adapter for {@link org.apache.ignite.compute.ComputeTaskSplitAdapter}
 * overriding {@code split(...)} method to return singleton with self instance.
 * This adapter should be used for tasks that always splits to a single task.
 * @param <T> Type of the task execution argument.
 * @param <R> Type of the task result returning from {@link org.apache.ignite.compute.ComputeTask#reduce(List)} method.
 */
public abstract class GridTaskSingleJobSplitAdapter<T, R> extends ComputeTaskSplitAdapter<T, R> {
    /** Empty constructor. */
    protected GridTaskSingleJobSplitAdapter() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(final int gridSize, final T arg) throws GridException {
        return Collections.singleton(new ComputeJobAdapter() {
            @Override public Object execute() throws GridException {
                return executeJob(gridSize, arg);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public R reduce(List<ComputeJobResult> results) throws GridException {
        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.isCancelled())
            throw new GridException("Reduce receives failed job.");

        return res.getData();
    }

    /**
     * Executes this task's job.
     *
     * @param gridSize Number of available grid nodes. Note that returned number of
     *      jobs can be less, equal or greater than this grid size.
     * @param arg Task execution argument. Can be {@code null}.
     * @return Job execution result (possibly {@code null}). This result will be returned
     *      in {@link org.apache.ignite.compute.ComputeJobResult#getData()} method passed into
     *      {@link org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method into task on caller node.
     * @throws GridException If job execution caused an exception. This exception will be
     *      returned in {@link org.apache.ignite.compute.ComputeJobResult#getException()} method passed into
     *      {@link org.apache.ignite.compute.ComputeTask#result(org.apache.ignite.compute.ComputeJobResult, List)} method into task on caller node.
     *      If execution produces a {@link RuntimeException} or {@link Error}, then
     *      it will be wrapped into {@link GridException}.
     */
    protected abstract Object executeJob(int gridSize, T arg) throws GridException;
}
