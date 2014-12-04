/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.compute;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cancels given tasks sessions.
 */
@GridInternal
public class VisorComputeCancelSessionsTask extends VisorMultiNodeTask<Map<UUID, Set<IgniteUuid>>, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeCancelSessionsJob job(Map<UUID, Set<IgniteUuid>> arg) {
        return new VisorComputeCancelSessionsJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
        // No-op, just awaiting all jobs done.
        return null;
    }

    /**
     * Job that cancel tasks.
     */
    private static class VisorComputeCancelSessionsJob extends VisorJob<Map<UUID, Set<IgniteUuid>>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Map with task sessions IDs to cancel.
         */
        private VisorComputeCancelSessionsJob(Map<UUID, Set<IgniteUuid>> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Map<UUID, Set<IgniteUuid>> arg) throws GridException {
            Set<IgniteUuid> sesIds = arg.get(g.localNode().id());

            if (sesIds != null && !sesIds.isEmpty()) {
                IgniteCompute compute = g.compute(g.forLocal());

                Map<IgniteUuid, ComputeTaskFuture<Object>> futs = compute.activeTaskFutures();

                for (IgniteUuid sesId : sesIds) {
                    ComputeTaskFuture<Object> fut = futs.get(sesId);

                    if (fut != null)
                        fut.cancel();
                }
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeCancelSessionsJob.class, this);
        }
    }
}
