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

package org.apache.ignite.internal.util.future;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

/**
 * Util task that will execute ComputeTask on a given node.
 */
@GridInternal public class IgniteRemoteMapTask<T, R> extends ComputeTaskAdapter<T, R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final ClusterNode node;

    /** */
    private final ComputeTask<T, R> remoteTask;

    /**
     * @param node Target node.
     * @param remoteTask Delegate task.
     */
    public IgniteRemoteMapTask(ClusterNode node, ComputeTask<T, R> remoteTask) {
        this.node = node;
        this.remoteTask = remoteTask;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable T arg) throws IgniteException {

        for (ClusterNode node : subgrid) {
            if (node.equals(this.node))
                return Collections.singletonMap(new Job<>(remoteTask, arg), node);
        }

        throw new IgniteException("Node " + node + " is not present in subgrid.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public R reduce(List<ComputeJobResult> results) throws IgniteException {
        assert results.size() == 1;

        return results.get(0).getData();
    }

    /**
     *
     */
    private static class Job<T, R> extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final ComputeTask<T, R> remoteTask;

        /** */
        @Nullable private final T arg;

        /** */
        @Nullable private ComputeTaskFuture<R> future;

        /**
         * @param remoteTask Remote task.
         * @param arg Argument.
         */
        public Job(ComputeTask<T, R> remoteTask, @Nullable T arg) {
            this.remoteTask = remoteTask;
            this.arg = arg;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            if (future == null) {
                IgniteCompute compute = ignite.compute().withAsync();

                compute.execute(remoteTask, arg);

                ComputeTaskFuture<R> future = compute.future();

                this.future = future;

                jobCtx.holdcc();

                future.listen(new IgniteInClosure<IgniteFuture<R>>() {
                    @Override public void apply(IgniteFuture<R> future) {
                        jobCtx.callcc();
                    }
                });

                return null;
            }
            else {
                return future.get();
            }
        }
    }

}
