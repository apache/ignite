/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;
import static org.gridgain.grid.kernal.visor.compute.VisorComputeMonitoringHolder.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorComputeToggleMonitoringTask extends
    VisorMultiNodeTask<GridBiTuple<String, Boolean>, Boolean, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Boolean reduce(List<GridComputeJobResult> results) throws GridException {
        Collection<Boolean> toggles = new HashSet<>();

        for (GridComputeJobResult res: results)
            toggles.add(res.<Boolean>getData());

        // If all nodes return same result.
        return toggles.size() == 1;
    }

    /**
     * Job to toggle task monitoring on node.
     */
    private static class VisorComputeToggleMonitoringJob extends VisorJob<GridBiTuple<String, Boolean>, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Visor ID key and monitoring state flag.
         */
        private VisorComputeToggleMonitoringJob(GridBiTuple<String, Boolean> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(GridBiTuple<String, Boolean> arg) throws GridException {
            if (checkExplicitTaskMonitoring(g))
                return true;
            else {
                GridNodeLocalMap<String, VisorComputeMonitoringHolder> storage = g.nodeLocalMap();

                VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                if (holder == null) {
                    VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                    VisorComputeMonitoringHolder holderOld =
                        storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                    holder = holderOld == null ? holderNew : holderOld;
                }

                String visorKey = arg.get1();

                boolean state = arg.get2();

                // Set task monitoring state.
                if (state)
                    holder.startCollect(g, visorKey);
                else
                    holder.stopCollect(g, visorKey);

                // Return actual state. It could stay the same if events explicitly enabled in configuration.
                return g.allEventsUserRecordable(VISOR_TASK_EVTS);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeToggleMonitoringJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorComputeToggleMonitoringJob job(GridBiTuple<String, Boolean> arg) {
        return new VisorComputeToggleMonitoringJob(arg);
    }
}
