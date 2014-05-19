/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.GridNode;
import org.gridgain.grid.GridNodeMetrics;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.GridComputeJobResult;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorRunGcTask extends VisorMultiNodeTask<VisorRunGcTask.VisorRunGcArg,
    VisorRunGcTask.VisorRunGcTaskResult, VisorRunGcTask.VisorRunGcJobResult> {
    /**
     * Arguments for {@link VisorRunGcTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcArg extends VisorMultiNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        private final boolean dgc;

        /**
         * Create task argument with specified nodes Ids.
         *
         * @param ids Nodes Ids.
         * @param dgc Run DGC procedure on all caches.
         */
        public VisorRunGcArg(Set<UUID> ids, boolean dgc) {
            super(ids);

            this.dgc = dgc;
        }

        /**
         * @return Dgc.
         */
        public boolean dgc() {
            return dgc;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcTaskResult extends HashMap<UUID, VisorRunGcJobResult> {
        /** */
        private static final long serialVersionUID = 0L;
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcJob
        extends VisorJob<VisorRunGcArg, VisorRunGcJobResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorRunGcJob(VisorRunGcArg arg) {
            super(arg);
        }

        @Override
        protected VisorRunGcJobResult run(VisorRunGcArg arg) throws GridException {
            GridNode locNode = g.localNode();

            GridNodeMetrics m = locNode.metrics();

            long before = freeHeap(m);

            System.gc();

            if (arg.dgc())
                for (GridCache<?, ?> cache : g.cachesx(null))
                    cache.dgc();

            return new VisorRunGcJobResult(locNode.id(), before, freeHeap(m));
        }

        private long freeHeap(GridNodeMetrics m) {
            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcJobResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        private final UUID nid;

        private final long before;

        private final long after;

        public VisorRunGcJobResult(UUID nid, long before, long after) {
            this.nid = nid;
            this.before = before;
            this.after = after;
        }

        /**
         * @return Nid.
         */
        public UUID nid() {
            return nid;
        }

        /**
         * @return Before.
         */
        public long before() {
            return before;
        }

        /**
         * @return After.
         */
        public long after() {
            return after;
        }
    }

    @Override
    protected VisorJob<VisorRunGcArg, VisorRunGcJobResult> job(UUID nid, VisorRunGcArg arg) {
        return new VisorRunGcJob(arg);
    }

    @Nullable @Override public VisorRunGcTaskResult reduce(List<GridComputeJobResult> results) throws GridException {
        VisorRunGcTaskResult total = new VisorRunGcTaskResult();

        for (GridComputeJobResult res: results) {
            VisorRunGcJobResult jobRes = res.<VisorRunGcJobResult>getData();

            total.put(jobRes.nid, jobRes);
        }

        return total;
    }
}
