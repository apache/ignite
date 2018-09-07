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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorAdapter;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.cache.VisorCache;
import org.apache.ignite.internal.visor.cache.VisorMemoryMetrics;
import org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder;
import org.apache.ignite.internal.visor.igfs.VisorIgfs;
import org.apache.ignite.internal.visor.igfs.VisorIgfsEndpoint;
import org.apache.ignite.internal.visor.util.VisorExceptionWrapper;
import org.apache.ignite.lang.IgniteProductVersion;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isIgfsCache;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isSystemCache;
import static org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder.COMPUTE_MONITORING_HOLDER_KEY;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.EVT_MAPPER;
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
        res.getEvents().addAll(collectEvents(ignite, evtOrderKey, evtThrottleCntrKey, all, EVT_MAPPER));
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
                res.setTaskMonitoringEnabled(true);
            else {
                // Get current task monitoring state.
                res.setTaskMonitoringEnabled(arg.isTaskMonitoringEnabled());

                if (arg.isTaskMonitoringEnabled()) {
                    ConcurrentMap<String, VisorComputeMonitoringHolder> storage = ignite.cluster().nodeLocalMap();

                    VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                    if (holder == null) {
                        VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                        VisorComputeMonitoringHolder holderOld = storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                        holder = holderOld == null ? holderNew : holderOld;
                    }

                    // Enable task monitoring for new node in grid.
                    holder.startCollect(ignite, arg.getEventsOrderKey());

                    // Update current state after change (it may not changed in some cases).
                    res.setTaskMonitoringEnabled(ignite.allEventsUserRecordable(VISOR_TASK_EVTS));
                }
            }

            events0(res, arg.getEventsOrderKey(), arg.getEventsThrottleCounterKey(), arg.isTaskMonitoringEnabled());
        }
        catch (Exception e) {
            res.setEventsEx(new VisorExceptionWrapper(e));
        }
    }

    /**
     * @param ver Version to check.
     * @return {@code true} if found at least one compatible node with specified version.
     */
    protected boolean compatibleWith(IgniteProductVersion ver) {
        for (ClusterNode node : ignite.cluster().nodes())
            if (node.version().compareToIgnoreTimestamp(ver) <= 0)
                return true;

        return false;
    }

    /**
     * @param cacheName Cache name to check.
     * @return {@code true} if cache on local node is not a data cache or near cache disabled.
     */
    private boolean proxyCache(String cacheName) {
        GridDiscoveryManager discovery = ignite.context().discovery();

        ClusterNode locNode = ignite.localNode();

        return !(discovery.cacheAffinityNode(locNode, cacheName) || discovery.cacheNearNode(locNode, cacheName));
    }

    /**
     * Collect memory metrics.
     *
     * @param res Job result.
     */
    protected void memoryMetrics(VisorNodeDataCollectorJobResult res) {
        try {
            List<VisorMemoryMetrics> memoryMetrics = res.getMemoryMetrics();

            // TODO: Should be really fixed in IGNITE-7111.
            if (ignite.cluster().active()) {
                for (DataRegionMetrics m : ignite.dataRegionMetrics())
                    memoryMetrics.add(new VisorMemoryMetrics(m));
            }
        }
        catch (Exception e) {
            res.setMemoryMetricsEx(new VisorExceptionWrapper(e));
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

            Set<String> cacheGrps = arg.getCacheGroups();

            boolean all = F.isEmpty(cacheGrps);

            int partitions = 0;
            double total = 0;
            double ready = 0;

            List<VisorCache> resCaches = res.getCaches();

            for (String cacheName : cacheProc.cacheNames()) {
                if (proxyCache(cacheName))
                    continue;

                boolean sysCache = isSystemCache(cacheName);

                if (arg.getSystemCaches() || !(sysCache || isIgfsCache(cfg, cacheName))) {
                    long start0 = U.currentTimeMillis();

                    try {
                        GridCacheAdapter ca = cacheProc.internalCache(cacheName);

                        if (ca == null || !ca.context().started())
                            continue;

                        CacheMetrics cm = ca.localMetrics();

                        partitions += cm.getTotalPartitionsCount();

                        long partTotal = cm.getEstimatedRebalancingKeys();
                        long partReady = cm.getRebalancedKeys();

                        if (partReady >= partTotal)
                            partReady = Math.max(partTotal - 1, 0);

                        total += partTotal;
                        ready += partReady;

                        if (all || cacheGrps.contains(ca.configuration().getGroupName()))
                            resCaches.add(new VisorCache(ignite, ca, arg.isCollectCacheMetrics()));
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

            if (partitions == 0)
                res.setRebalance(-1);
            else
                res.setRebalance(total > 0 ? ready / total : 1);
        }
        catch (Exception e) {
            res.setCachesEx(new VisorExceptionWrapper(e));
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

                FileSystemConfiguration igfsCfg = igfs.configuration();

                if (proxyCache(igfsCfg.getDataCacheConfiguration().getName()) || proxyCache(igfsCfg.getMetaCacheConfiguration().getName()))
                    continue;

                try {
                    Collection<IpcServerEndpoint> endPoints = igfsProc.endpoints(igfs.name());

                    if (endPoints != null) {
                        for (IpcServerEndpoint ep : endPoints)
                            if (ep.isManagement())
                                res.getIgfsEndpoints().add(new VisorIgfsEndpoint(igfs.name(), ignite.name(),
                                    ep.getHost(), ep.getPort()));
                    }

                    res.getIgfss().add(new VisorIgfs(igfs));
                }
                finally {
                    if (debug)
                        log(ignite.log(), "Collected IGFS: " + igfs.name(), getClass(), start0);
                }
            }
        }
        catch (Exception e) {
            res.setIgfssEx(new VisorExceptionWrapper(e));
        }
    }

    /**
     * Collect persistence metrics.
     *
     * @param res Job result.
     */
    protected void persistenceMetrics(VisorNodeDataCollectorJobResult res) {
        try {
            res.setPersistenceMetrics(new VisorPersistenceMetrics(ignite.dataStorageMetrics()));
        }
        catch (Exception e) {
            res.setPersistenceMetricsEx(new VisorExceptionWrapper(e));
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
        res.setGridName(ignite.name());

        GridCachePartitionExchangeManager<Object, Object> exchange = ignite.context().cache().context().exchange();

        res.setReadyAffinityVersion(new VisorAffinityTopologyVersion(exchange.readyAffinityVersion()));
        res.setHasPendingExchange(exchange.hasPendingExchange());

        res.setTopologyVersion(ignite.cluster().topologyVersion());

        long start0 = U.currentTimeMillis();

        events(res, arg);

        if (debug)
            start0 = log(ignite.log(), "Collected events", getClass(), start0);

        memoryMetrics(res);

        if (debug)
            start0 = log(ignite.log(), "Collected memory metrics", getClass(), start0);

        caches(res, arg);

        if (debug)
            start0 = log(ignite.log(), "Collected caches", getClass(), start0);

        igfs(res);

        if (debug)
            start0 = log(ignite.log(), "Collected igfs", getClass(), start0);

        persistenceMetrics(res);

        if (debug)
            log(ignite.log(), "Collected persistence metrics", getClass(), start0);

        res.setErrorCount(ignite.context().exceptionRegistry().errorCount());

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorNodeDataCollectorJob.class, this);
    }
}
