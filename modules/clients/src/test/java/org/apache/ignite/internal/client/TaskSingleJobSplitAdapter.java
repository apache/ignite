/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;

/**
 * Adapter for {@link org.apache.ignite.compute.ComputeTaskSplitAdapter}
 * overriding {@code split(...)} method to return singleton with self instance.
 * This adapter should be used for tasks that always splits to a single task.
 * @param <T> Type of the task execution argument.
 * @param <R> Type of the task result returning from {@link org.apache.ignite.compute.ComputeTask#reduce(List)} method.
 */
public abstract class TaskSingleJobSplitAdapter<T, R> extends ComputeTaskSplitAdapter<T, R> {
    /** Empty constructor. */
    protected TaskSingleJobSplitAdapter() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(final int gridSize, final T arg) {
        return Collections.singleton(new ComputeJobAdapter() {
            @Override public Object execute() {
                return executeJob(gridSize, arg);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public R reduce(List<ComputeJobResult> results) {
        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.isCancelled())
            throw new IgniteException("Reduce receives failed job.");

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
     */
    protected abstract Object executeJob(int gridSize, T arg);
}