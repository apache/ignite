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

@GridInternal public class IgniteRemoteMapTask<T, R> extends ComputeTaskAdapter<T, R> {

    private final ClusterNode node;

    private final ComputeTask<T, R> remoteTask;

    public IgniteRemoteMapTask(ClusterNode node, ComputeTask<T, R> remoteTask) {
        this.node = node;
        this.remoteTask = remoteTask;
    }

    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable T arg) throws IgniteException {

        for (ClusterNode node : subgrid) {
            if (node.equals(this.node))
                return Collections.singletonMap(new Job<>(remoteTask, arg), node);
        }

        throw new IgniteException("Node " + node + " is not present in subgrid.");
    }

    @Nullable @Override public R reduce(List<ComputeJobResult> results) throws IgniteException {
        assert results.size() == 1;

        return results.get(0).getData();
    }

    private static class Job<T, R> extends ComputeJobAdapter {
        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        @IgniteInstanceResource
        private Ignite ignite;

        private final ComputeTask<T, R> remoteTask;

        @Nullable private ComputeTaskFuture<R> future;

        @Nullable private final T arg;

        public Job(ComputeTask<T, R> remoteTask, @Nullable T arg) {
            this.remoteTask = remoteTask;
            this.arg = arg;
        }

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
