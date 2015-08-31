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

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder;
import org.apache.ignite.internal.visor.igfs.VisorIgfs;
import org.apache.ignite.internal.visor.igfs.VisorIgfsEndpoint;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder.COMPUTE_MONITORING_HOLDER_KEY;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.checkExplicitTaskMonitoring;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.collectEvents;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.log;

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

    /**
     * Collect events.
     *
     * @param res Job result.
     * @param evtOrderKey Unique key to take last order key from node local map.
     * @param evtThrottleCntrKey Unique key to take throttle count from node local map.
     * @param all If {@code true} then collect all events otherwise collect only non task events.
     */
    protected void events0(VisorNodeDataCollectorJobResult res, String evtOrderKey, String evtThrottleCntrKey,
        final boolean all) {
        res.events().addAll(collectEvents(ignite, evtOrderKey, evtThrottleCntrKey, all));
    }

    /**
     * Collect events.
     *
     * @param res Job result.
     * @param arg Task argument.
     */
    protected void events(VisorNodeDataCollectorJobResult res, VisorNodeDataCollectorTaskArg arg) {
        try {
            // Visor events explicitly enabled in configuration.
            if (checkExplicitTaskMonitoring(ignite))
                res.taskMonitoringEnabled(true);
            else {
                // Get current task monitoring state.
                res.taskMonitoringEnabled(arg.taskMonitoringEnabled());

                if (arg.taskMonitoringEnabled()) {
                    ConcurrentMap<String, VisorComputeMonitoringHolder> storage = ignite.cluster().nodeLocalMap();

                    VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                    if (holder == null) {
                        VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                        VisorComputeMonitoringHolder holderOld = storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                        holder = holderOld == null ? holderNew : holderOld;
                    }

                    // Enable task monitoring for new node in grid.
                    holder.startCollect(ignite, arg.eventsOrderKey());

                    // Update current state after change (it may not changed in some cases).
                    res.taskMonitoringEnabled(ignite.allEventsUserRecordable(VISOR_TASK_EVTS));
                }
            }

            events0(res, arg.eventsOrderKey(), arg.eventsThrottleCounterKey(), arg.taskMonitoringEnabled());
        }
        catch (Exception eventsEx) {
            res.eventsEx(eventsEx);
        }
    }

    /**
     * Collect caches.
     *
     * @param res Job result.
     * @param arg Task argument.
     */
    protected void caches(VisorNodeDataCollectorJobResult res, VisorNodeDataCollectorTaskArg arg) {
        try {
            IgniteConfiguration cfg = ignite.configuration();

            GridCacheProcessor cacheProc = ignite.context().cache();

            for (String cacheName : cacheProc.cacheNames()) {
                if (arg.systemCaches() || !(isSystemCache(cacheName) || isIgfsCache(cfg, cacheName))) {
                    long start0 = U.currentTimeMillis();

                    try {
                        VisorCache cache = new VisorCache().from(ignite, cacheName, arg.sample());

                        if (cache != null)
                            res.caches().add(cache);
                    }
                    catch(IllegalStateException | IllegalArgumentException e) {
                        if (debug && ignite.log() != null)
                            ignite.log().error("Ignored cache: " + cacheName, e);
                    }
                    finally {
                        if (debug)
                            log(ignite.log(), "Collected cache: " + cacheName, getClass(), start0);
                    }
                }
            }
        }
        catch (Exception cachesEx) {
            res.cachesEx(cachesEx);
        }
    }

    /**
     * Collect IGFSs.
     *
     * @param res Job result.
     */
    protected void igfs(VisorNodeDataCollectorJobResult res) {
        try {
            IgfsProcessorAdapter igfsProc = ignite.context().igfs();

            for (IgniteFileSystem igfs : igfsProc.igfss()) {
                long start0 = U.currentTimeMillis();

                try {
                    Collection<IpcServerEndpoint> endPoints = igfsProc.endpoints(igfs.name());

                    if (endPoints != null) {
                        for (IpcServerEndpoint ep : endPoints)
                            if (ep.isManagement())
                                res.igfsEndpoints().add(new VisorIgfsEndpoint(igfs.name(), ignite.name(),
                                    ep.getHost(), ep.getPort()));
                    }

                    res.igfss().add(VisorIgfs.from(igfs));
                }
                finally {
                    if (debug)
                        log(ignite.log(), "Collected IGFS: " + igfs.name(), getClass(), start0);
                }
            }
        }
        catch (Exception igfssEx) {
            res.igfssEx(igfssEx);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorTaskArg arg) {
        return run(new VisorNodeDataCollectorJobResult(), arg);
    }

    /**
     * Execution logic of concrete job.
     *
     * @param res Result response.
     * @param arg Job argument.
     * @return Job result.
     */
    protected VisorNodeDataCollectorJobResult run(VisorNodeDataCollectorJobResult res,
        VisorNodeDataCollectorTaskArg arg) {
        res.gridName(ignite.name());

        res.topologyVersion(ignite.cluster().topologyVersion());

        long start0 = U.currentTimeMillis();

        events(res, arg);

        if (debug)
            start0 = log(ignite.log(), "Collected events", getClass(), start0);

        caches(res, arg);

        if (debug)
            start0 = log(ignite.log(), "Collected caches", getClass(), start0);

        igfs(res);

        if (debug)
            log(ignite.log(), "Collected igfs", getClass(), start0);

        res.errorCount(ignite.context().exceptionRegistry().errorCount());

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorJob.class, this);
    }
}