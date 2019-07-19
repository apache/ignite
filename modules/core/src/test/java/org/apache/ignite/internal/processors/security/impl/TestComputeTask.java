package org.apache.ignite.internal.processors.security.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.lang.IgniteRunnable;
import org.jetbrains.annotations.Nullable;

/** . */
public class TestComputeTask implements ComputeTask<Object, Object> {
    /** . */
    private final IgniteRunnable r;

    /** . */
    public TestComputeTask(IgniteRunnable r) {
        this.r = r;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) throws IgniteException {
        return Collections.singletonMap(
            new ComputeJob() {
                @Override public void cancel() {
                    // no-op
                }

                @Override public Object execute() {
                    r.run();

                    return null;
                }
            }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
        );
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res,
        List<ComputeJobResult> rcvd) throws IgniteException {
        if (res.getException() != null)
            throw res.getException();

        return ComputeJobResultPolicy.REDUCE;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
        return null;
    }
}
