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
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Ping other node.
 */
@GridInternal
public class VisorNodePingTask extends VisorOneNodeTask<UUID, GridTuple3<Boolean, Long, Long>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorPingJob job(UUID arg) {
        return new VisorPingJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected GridTuple3<Boolean, Long, Long> reduce0(List<ComputeJobResult> results) throws IgniteCheckedException {
        try {
            return super.reduce0(results);
        }
        catch (ClusterTopologyException ignored) {
            return new GridTuple3<>(false, -1L, -1L);
        }
    }

    /**
     * Job that ping node.
     */
    private static class VisorPingJob extends VisorJob<UUID, GridTuple3<Boolean, Long, Long>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Node ID to ping.
         * @param debug Debug flag.
         */
        protected VisorPingJob(UUID arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected GridTuple3<Boolean, Long, Long> run(UUID nodeToPing) throws IgniteCheckedException {
            long start = System.currentTimeMillis();

            return new GridTuple3<>(g.pingNode(nodeToPing), start, System.currentTimeMillis());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorPingJob.class, this);
        }
    }
}
