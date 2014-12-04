/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.proto;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Hadoop protocol task adapter.
 */
public abstract class GridHadoopProtocolTaskAdapter<R> implements ComputeTask<GridHadoopProtocolTaskArguments, R> {
    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable GridHadoopProtocolTaskArguments arg) throws GridException {
        return Collections.singletonMap(new Job(arg), subgrid.get(0));
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
        throws GridException {
        return ComputeJobResultPolicy.REDUCE;
    }

    /** {@inheritDoc} */
    @Nullable @Override public R reduce(List<ComputeJobResult> results) throws GridException {
        if (!F.isEmpty(results)) {
            ComputeJobResult res = results.get(0);

            return res.getData();
        }
        else
            return null;
    }

    /**
     * Job wrapper.
     */
    private class Job implements ComputeJob {
        /** */
        private static final long serialVersionUID = 0L;

        @IgniteInstanceResource
        private Ignite ignite;

        @SuppressWarnings("UnusedDeclaration")
        @IgniteJobContextResource
        private ComputeJobContext jobCtx;

        /** Argument. */
        private final GridHadoopProtocolTaskArguments args;

        /**
         * Constructor.
         *
         * @param args Job argument.
         */
        private Job(GridHadoopProtocolTaskArguments args) {
            this.args = args;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() throws GridException {
            return run(jobCtx, ignite.hadoop(), args);
        }
    }

    /**
     * Run the task.
     *
     * @param jobCtx Job context.
     * @param hadoop Hadoop facade.
     * @param args Arguments.
     * @return Job result.
     * @throws GridException If failed.
     */
    public abstract R run(ComputeJobContext jobCtx, GridHadoop hadoop, GridHadoopProtocolTaskArguments args)
        throws GridException;
}
