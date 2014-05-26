/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorRunGcTask extends VisorMultiNodeTask<VisorRunGcTask.VisorRunGcArg,
    VisorRunGcTask.VisorRunGcTaskResult, VisorBeforeAfterResult> {
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
    public static class VisorRunGcTaskResult extends HashMap<UUID, VisorBeforeAfterResult> {
        /** */
        private static final long serialVersionUID = 0L;
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcJob
        extends VisorJob<VisorRunGcArg, VisorBeforeAfterResult> {
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
        protected VisorBeforeAfterResult run(VisorRunGcArg arg) throws GridException {
            GridNode locNode = g.localNode();

            long before = freeHeap(locNode.metrics());

            System.gc();

            if (arg.dgc())
                for (GridCache<?, ?> cache : g.cachesx(null))
                    cache.dgc();

            return new VisorBeforeAfterResult(before, freeHeap(locNode.metrics()));
        }

        private long freeHeap(GridNodeMetrics m) {
            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }
    }

    @Override
    protected VisorJob<VisorRunGcArg, VisorBeforeAfterResult> job(UUID nid, VisorRunGcArg arg) {
        return new VisorRunGcJob(arg);
    }

    @Nullable @Override public VisorRunGcTaskResult reduce(List<GridComputeJobResult> results) throws GridException {
        VisorRunGcTaskResult total = new VisorRunGcTaskResult();

        for (GridComputeJobResult res: results) {
            VisorBeforeAfterResult jobRes = res.getData();

            total.put(res.getNode().id(), jobRes);
        }

        return total;
    }
}
