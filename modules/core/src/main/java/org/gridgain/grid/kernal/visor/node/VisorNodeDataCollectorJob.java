package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.kernal.visor.cache.*;
import org.gridgain.grid.kernal.visor.compute.*;
import org.gridgain.grid.kernal.visor.ggfs.*;
import org.gridgain.grid.kernal.visor.streamer.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.kernal.visor.compute.VisorComputeMonitoringHolder.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Job that collects data from node.
 */
public class VisorNodeDataCollectorJob extends VisorJob<VisorNodeDataCollectorTaskArg, VisorNodeDataCollectorJobResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Create job with given argument.
     *
     * @param arg Job argument.
     */
    public VisorNodeDataCollectorJob(VisorNodeDataCollectorTaskArg arg) {
        super(arg);
    }

    /** Collect events. */
    private void events(VisorNodeDataCollectorJobResult res, VisorNodeDataCollectorTaskArg arg) {
        try {
            // Visor events explicitly enabled in configuration.
            if (checkExplicitTaskMonitoring(g))
                res.taskMonitoringEnabled(true);
            else {
                // Get current task monitoring state.
                res.taskMonitoringEnabled(arg.taskMonitoringEnabled());

                if (arg.taskMonitoringEnabled()) {
                    ClusterNodeLocalMap<String, VisorComputeMonitoringHolder> storage = g.nodeLocalMap();

                    VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                    if (holder == null) {
                        VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                        VisorComputeMonitoringHolder holderOld = storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                        holder = holderOld == null ? holderNew : holderOld;
                    }

                    // Enable task monitoring for new node in grid.
                    holder.startCollect(g, arg.eventsOrderKey());

                    // Update current state after change (it may not changed in some cases).
                    res.taskMonitoringEnabled(g.allEventsUserRecordable(VISOR_TASK_EVTS));
                }
            }

            res.events().addAll(collectEvents(g, arg.eventsOrderKey(), arg.eventsThrottleCounterKey(),
                arg.taskMonitoringEnabled()));
        }
        catch (Throwable eventsEx) {
            res.eventsEx(eventsEx);
        }
    }

    /** Collect caches. */
    private void caches(VisorNodeDataCollectorJobResult res, VisorNodeDataCollectorTaskArg arg) {
        try {
            IgniteConfiguration cfg = g.configuration();

            for (GridCache cache : g.cachesx()) {
                String cacheName = cache.name();

                if (arg.systemCaches() || !(isSystemCache(cacheName) || isGgfsCache(cfg, cacheName)))
                    res.caches().add(VisorCache.from(g, cache, arg.sample()));
            }
        }
        catch (Throwable cachesEx) {
            res.cachesEx(cachesEx);
        }
    }

    /** Collect GGFS. */
    private void ggfs(VisorNodeDataCollectorJobResult res) {
        try {
            GridGgfsProcessorAdapter ggfsProc = ((GridKernal)g).context().ggfs();

            for (IgniteFs ggfs : ggfsProc.ggfss()) {
                Collection<GridIpcServerEndpoint> endPoints = ggfsProc.endpoints(ggfs.name());

                if (endPoints != null) {
                    for (GridIpcServerEndpoint ep : endPoints)
                        if (ep.isManagement())
                            res.ggfsEndpoints().add(new VisorGgfsEndpoint(ggfs.name(), g.name(),
                                ep.getHost(), ep.getPort()));
                }

                res.ggfss().add(VisorGgfs.from(ggfs));
            }
        }
        catch (Throwable ggfssEx) {
            res.ggfssEx(ggfssEx);
        }
    }

    /** Collect streamers. */
    private void streamers(VisorNodeDataCollectorJobResult res) {
        try {
            StreamerConfiguration[] cfgs = g.configuration().getStreamerConfiguration();

            if (cfgs != null) {
                for (StreamerConfiguration cfg : cfgs)
                    res.streamers().add(VisorStreamer.from(g.streamer(cfg.getName())));
            }
        }
        catch (Throwable streamersEx) {
            res.streamersEx(streamersEx);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorTaskArg arg) throws GridException {
        return run(new VisorNodeDataCollectorJobResult(), arg);
    }

    protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorJobResult res,
        VisorNodeDataCollectorTaskArg arg) throws GridException {
        res.gridName(g.name());

        res.topologyVersion(g.topologyVersion());

        events(res, arg);

        caches(res, arg);

        ggfs(res);

        streamers(res);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorJob.class, this);
    }
}
