/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Collects current Grid state mostly topology and metrics.
 */
@GridInternal
public class VisorNodeDataCollectorTask extends VisorMultiNodeTask<VisorNodeDataCollectorTaskArg,
    VisorNodeDataCollectorTaskResult, VisorNodeDataCollectorJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable VisorTaskArgument<VisorNodeDataCollectorTaskArg> arg) throws GridException {
        assert arg != null;

        taskArg = arg.argument();

        Collection<ClusterNode> nodes = g.nodes();

        Map<ComputeJob, ClusterNode> map = U.newHashMap(nodes.size());

        // Collect data from ALL nodes.
        for (ClusterNode node : nodes)
            map.put(job(taskArg), node);

        return map;
    }

    /** {@inheritDoc} */
    @Override protected VisorNodeDataCollectorJob job(VisorNodeDataCollectorTaskArg arg) {
        return new VisorNodeDataCollectorJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public VisorNodeDataCollectorTaskResult reduce(List<ComputeJobResult> results) throws GridException {
        return reduce(new VisorNodeDataCollectorTaskResult(), results);
    }

    protected VisorNodeDataCollectorTaskResult reduce(VisorNodeDataCollectorTaskResult taskResult,
        List<ComputeJobResult> results) throws GridException {
        for (ComputeJobResult res : results) {
            VisorNodeDataCollectorJobResult jobResult = res.getData();

            if (jobResult != null) {
                UUID nid = res.getNode().id();

                GridException unhandledEx = res.getException();

                if (unhandledEx == null)
                    reduceJobResult(taskResult, jobResult, nid);
                else {
                    // Ignore nodes that left topology.
                    if (!(unhandledEx instanceof GridEmptyProjectionException))
                        taskResult.unhandledEx().put(nid, unhandledEx);
                }
            }
        }

        return taskResult;
    }

    protected void reduceJobResult(VisorNodeDataCollectorTaskResult taskResult,
        VisorNodeDataCollectorJobResult jobResult, UUID nid) throws GridException {
        taskResult.gridNames().put(nid, jobResult.gridName());

        taskResult.topologyVersions().put(nid, jobResult.topologyVersion());

        taskResult.taskMonitoringEnabled().put(nid, jobResult.taskMonitoringEnabled());

        if (!jobResult.events().isEmpty())
            taskResult.events().addAll(jobResult.events());

        if (jobResult.eventsEx() != null)
            taskResult.eventsEx().put(nid, jobResult.eventsEx());

        if (!jobResult.caches().isEmpty())
            taskResult.caches().put(nid, jobResult.caches());

        if (jobResult.cachesEx() != null)
            taskResult.cachesEx().put(nid, jobResult.cachesEx());

        if (!jobResult.streamers().isEmpty())
            taskResult.streamers().put(nid, jobResult.streamers());

        if (jobResult.streamersEx() != null)
            taskResult.streamersEx().put(nid, jobResult.streamersEx());

        if (!jobResult.ggfss().isEmpty())
            taskResult.ggfss().put(nid, jobResult.ggfss());

        if (!jobResult.ggfsEndpoints().isEmpty())
            taskResult.ggfsEndpoints().put(nid, jobResult.ggfsEndpoints());

        if (jobResult.ggfssEx() != null)
            taskResult.ggfssEx().put(nid, jobResult.ggfssEx());
    }
}
