/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.internal.visor.cache.*;
import org.apache.ignite.internal.visor.compute.*;
import org.apache.ignite.internal.visor.ggfs.*;
import org.apache.ignite.internal.visor.streamer.*;
import org.apache.ignite.internal.util.ipc.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder.*;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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
     * @param debug Debug flag.
     */
    public VisorNodeDataCollectorJob(VisorNodeDataCollectorTaskArg arg, boolean debug) {
        super(arg, debug);
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

                if (arg.systemCaches() || !(isSystemCache(cacheName) || isGgfsCache(cfg, cacheName))) {
                    long start0 = U.currentTimeMillis();

                    try {
                        res.caches().add(VisorCache.from(g, cache, arg.sample()));
                    }
                    finally {
                        if (debug)
                            log(g.log(), "Collected cache: " + cache.name(), getClass(), start0);
                    }
                }
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
                long start0 = U.currentTimeMillis();

                try {
                    Collection<GridIpcServerEndpoint> endPoints = ggfsProc.endpoints(ggfs.name());

                    if (endPoints != null) {
                        for (GridIpcServerEndpoint ep : endPoints)
                            if (ep.isManagement())
                                res.ggfsEndpoints().add(new VisorGgfsEndpoint(ggfs.name(), g.name(),
                                    ep.getHost(), ep.getPort()));
                    }

                    res.ggfss().add(VisorGgfs.from(ggfs));
                }
                finally {
                    if (debug)
                        log(g.log(), "Collected GGFS: " + ggfs.name(), getClass(), start0);
                }
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
                for (StreamerConfiguration cfg : cfgs) {
                    long start0 = U.currentTimeMillis();

                    try {
                        res.streamers().add(VisorStreamer.from(g.streamer(cfg.getName())));
                    }
                    finally {
                        if (debug)
                            log(g.log(), "Collected streamer: " + cfg.getName(), getClass(), start0);
                    }
                }
            }
        }
        catch (Throwable streamersEx) {
            res.streamersEx(streamersEx);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorTaskArg arg) throws IgniteCheckedException {
        return run(new VisorNodeDataCollectorJobResult(), arg);
    }

    protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorJobResult res,
        VisorNodeDataCollectorTaskArg arg) throws IgniteCheckedException {
        res.gridName(g.name());

        res.topologyVersion(g.topologyVersion());

        long start0 = U.currentTimeMillis();

        events(res, arg);

        if (debug)
            start0 = log(g.log(), "Collected events", getClass(), start0);

        caches(res, arg);

        if (debug)
            start0 = log(g.log(), "Collected caches", getClass(), start0);

        ggfs(res);

        if (debug)
            start0 = log(g.log(), "Collected ggfs", getClass(), start0);

        streamers(res);

        if (debug)
            log(g.log(), "Collected streamers", getClass(), start0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorJob.class, this);
    }
}
