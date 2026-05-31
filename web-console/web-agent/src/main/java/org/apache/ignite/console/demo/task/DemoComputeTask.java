

package org.apache.ignite.console.demo.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Simple compute task.
 */
public class DemoComputeTask implements ComputeTask<Void, Integer>{
    /** */
    private static final long serialVersionUID = 0L;

    /** Random generator. */
    private static final Random rnd = new Random();

    /** {@inheritDoc} */
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(
        List<ClusterNode> subgrid,
        @Nullable Void arg
    ) throws IgniteException {
        HashMap<ComputeJob, ClusterNode> map = new HashMap<>(subgrid.size());

        for (ClusterNode node: subgrid) {
            for (int i = 0; i < Math.max(1, rnd.nextInt(5)); i++)
                map.put(new DemoComputeJob(), node);
        }

        return map;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
        return ComputeJobResultPolicy.REDUCE;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
        int sum = 0;

        for (ComputeJobResult r: results) {
            if (!r.isCancelled() && r.getException() == null) {
                int jobRes = r.getData();

                sum += jobRes;
            }
        }

        return sum;
    }

    /**
     * Simple compute job.
     */
    private static class DemoComputeJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            try {
                Thread.sleep(rnd.nextInt(50));

                return rnd.nextInt(10000);
            }
            catch (InterruptedException e) {
                // Restore interrupt status
                Thread.currentThread().interrupt();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DemoComputeJob.class, this);
        }
    }
}
