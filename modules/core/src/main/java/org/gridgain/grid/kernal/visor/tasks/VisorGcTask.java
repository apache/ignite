/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorGcTask extends VisorMultiNodeTask<Boolean, Map<UUID, GridBiTuple<Long, Long>>,
    GridBiTuple<Long, Long>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorGcJob job(Boolean arg) {
        return new VisorGcJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<UUID, GridBiTuple<Long, Long>> reduce(List<GridComputeJobResult> results)
        throws GridException {
        Map<UUID, GridBiTuple<Long, Long>> total = new HashMap<>();

        for (GridComputeJobResult res: results) {
            GridBiTuple<Long, Long> jobRes = res.getData();

            total.put(res.getNode().id(), jobRes);
        }

        return total;
    }

    /** Job that perform GC on node. */
    private static class VisorGcJob extends VisorJob<Boolean, GridBiTuple<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        private VisorGcJob(Boolean arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected GridBiTuple<Long, Long> run(Boolean dgc) throws GridException {
            GridNode locNode = g.localNode();

            long before = freeHeap(locNode);

            System.gc();

            if (dgc)
                for (GridCache<?, ?> cache : g.cachesx())
                    cache.dgc();

            return new GridBiTuple<>(before, freeHeap(locNode));
        }

        /**
         * @param node Node.
         * @return Current free heap.
         */
        private long freeHeap(GridNode node) {
            final GridNodeMetrics m = node.metrics();

            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGcJob.class, this);
        }
    }
}
