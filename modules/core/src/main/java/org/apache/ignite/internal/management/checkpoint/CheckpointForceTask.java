package org.apache.ignite.internal.management.checkpoint;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** Checkpoint force task. */
public class CheckpointForceTask extends VisorMultiNodeTask<CheckpointForceCommandArg, String, String> {
    /** */
    private static final long serialVersionUID = 0;

    /** {@inheritDoc} */
    @Override protected VisorJob<CheckpointForceCommandArg, String> job(CheckpointForceCommandArg arg) {
        return new CheckpointForceJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable String reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null)
                throw res.getException();
        }

        return "Checkpoint triggered on all nodes";
    }

    /** Checkpoint force job. */
    private static class CheckpointForceJob extends VisorJob<CheckpointForceCommandArg, String> {
        /** */
        private static final long serialVersionUID = 0;

        /** */
        protected CheckpointForceJob(@Nullable CheckpointForceCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(@Nullable CheckpointForceCommandArg arg) throws IgniteException {
            String reason = arg != null && arg.reason() != null ? arg.reason() : "control.sh";
            boolean waitForFinish = arg != null && arg.waitForFinish();
            Long timeout = arg != null ? arg.timeout() : null;

            try {
                GridKernalContext cctx = ignite.context();

                GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.cache().context().database();

                //GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();
                CheckpointProgress checkpointFuture = dbMgr.forceCheckpoint(reason);

                if (waitForFinish) {
                    if (timeout != null && timeout > 0) {
                        checkpointFuture.futureFor(CheckpointState.FINISHED).get(timeout, TimeUnit.MILLISECONDS);
                    } else {
                        checkpointFuture.futureFor(CheckpointState.FINISHED).get();
                    }
                    return "Checkpoint completed on node: " + ignite.localNode().id();
                } else {
                    return "Checkpoint triggered on node: " + ignite.localNode().id();
                }
            } catch (Exception e) {
                throw new IgniteException("Failed to force checkpoint on node: " + ignite.localNode().id(), e);
            }
        }
    }
}