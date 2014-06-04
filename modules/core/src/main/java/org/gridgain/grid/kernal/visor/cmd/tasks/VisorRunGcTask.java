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
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorRunGcTask extends VisorMultiNodeTask<VisorRunGcTask.VisorRunGcArg,
    Map<UUID, T2<Long, Long>>, T2<Long, Long>> {
    /**
     * Arguments for {@link VisorRunGcTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorRunGcArg extends VisorMultiNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** Whether to run DGC on all caches. */
        private final boolean dgc;

        /**
         * Create task argument with specified nodes Ids.
         *
         * @param nids Nodes Ids.
         * @param dgc Whether to run DGC on all caches.
         */
        public VisorRunGcArg(Set<UUID> nids, boolean dgc) {
            super(nids);

            this.dgc = dgc;
        }
    }

    /** Job that perform GC on node. */
    private static class VisorRunGcJob extends VisorJob<VisorRunGcArg, T2<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        private VisorRunGcJob(VisorRunGcArg arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected T2<Long, Long> run(VisorRunGcArg arg) throws GridException {
            GridNode locNode = g.localNode();

            long before = freeHeap(locNode);

            System.gc();

            if (arg.dgc)
                for (GridCache<?, ?> cache : g.cachesx())
                    cache.dgc();

            return new T2<>(before, freeHeap(locNode));
        }

        private long freeHeap(GridNode node) {
            final GridNodeMetrics m = node.metrics();

            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorRunGcJob job(VisorRunGcArg arg) {
        return new VisorRunGcJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<UUID, T2<Long, Long>> reduce(List<GridComputeJobResult> results) throws GridException {
        Map<UUID, T2<Long, Long>> total = new HashMap<>();

        for (GridComputeJobResult res: results) {
            T2<Long, Long> jobRes = res.getData();

            total.put(res.getNode().id(), jobRes);
        }

        return total;
    }
}
