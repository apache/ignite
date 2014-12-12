/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorNodeGcTask extends VisorMultiNodeTask<Void, Map<UUID, IgniteBiTuple<Long, Long>>,
    IgniteBiTuple<Long, Long>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorGcJob job(Void arg) {
        return new VisorGcJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected Map<UUID, IgniteBiTuple<Long, Long>> reduce0(List<ComputeJobResult> results)
        throws IgniteCheckedException {
        Map<UUID, IgniteBiTuple<Long, Long>> total = new HashMap<>();

        for (ComputeJobResult res: results) {
            IgniteBiTuple<Long, Long> jobRes = res.getData();

            total.put(res.getNode().id(), jobRes);
        }

        return total;
    }

    /** Job that perform GC on node. */
    private static class VisorGcJob extends VisorJob<Void, IgniteBiTuple<Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Formal task argument.
         * @param debug Debug flag.
         */
        private VisorGcJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Long, Long> run(Void arg) throws IgniteCheckedException {
            ClusterNode locNode = g.localNode();

            long before = freeHeap(locNode);

            System.gc();

            return new IgniteBiTuple<>(before, freeHeap(locNode));
        }

        /**
         * @param node Node.
         * @return Current free heap.
         */
        private long freeHeap(ClusterNode node) {
            final ClusterNodeMetrics m = node.metrics();

            return m.getHeapMemoryMaximum() - m.getHeapMemoryUsed();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGcJob.class, this);
        }
    }
}
