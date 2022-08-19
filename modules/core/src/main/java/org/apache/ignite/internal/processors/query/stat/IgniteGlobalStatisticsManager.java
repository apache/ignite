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

package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnGlobalDataViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnLocalDataViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.StatisticsColumnPartitionDataViewWalker;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsKeyMessage;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsObjectData;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsRequest;
import org.apache.ignite.internal.processors.query.stat.messages.StatisticsResponse;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnConfigurationView;
import org.apache.ignite.internal.processors.query.stat.view.StatisticsColumnGlobalDataView;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

/**
 * Global statistics manager. Cache global statistics and collect it.
 */
public class IgniteGlobalStatisticsManager implements GridMessageListener {
    /** */
    private static final String STAT_GLOBAL_VIEW_NAME = "statisticsGlobalData";

    /** */
    private static final String STAT_GLOBAL_VIEW_DESCRIPTION = "Global statistics.";

    /** Statistics manager. */
    private final IgniteStatisticsManagerImpl statMgr;

    /** Pool to process statistics requests. */
    private final IgniteThreadPoolExecutor mgmtPool;

    /** Discovery manager to get server node list to statistics master calculation. */
    private final GridDiscoveryManager discoMgr;

    /** Cluster state processor. */
    private final GridClusterStateProcessor cluster;

    /** Cache partition exchange manager. */
    private final GridCachePartitionExchangeManager<?, ?> exchange;

    /** Helper to transform or generate statistics related messages. */
    private final IgniteStatisticsHelper helper;

    /** Grid io manager to exchange global and local statistics. */
    private final GridIoManager ioMgr;

    /** Cache for global statistics. */
    private final ConcurrentMap<StatisticsKey, CacheEntry<ObjectStatisticsImpl>> globalStatistics =
        new ConcurrentHashMap<>();

    /** Incoming requests which should be served after local statistics collection finish. */
    private final ConcurrentMap<StatisticsKey, Collection<StatisticsAddressedRequest>> inLocalRequests =
        new ConcurrentHashMap<>();

    /** Incoming requests which should be served after global statistics collection finish. */
    private final ConcurrentMap<StatisticsKey, Collection<StatisticsAddressedRequest>> inGloblaRequests =
        new ConcurrentHashMap<>();

    /** Outcoming global collection requests. */
    private final ConcurrentMap<StatisticsKey, StatisticsGatheringContext> curCollections = new ConcurrentHashMap<>();

    /** Outcoming global statistics requests to request id. */
    private final ConcurrentMap<StatisticsKey, UUID> outGlobalStatisticsRequests = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /** Started flag. */
    private boolean started;

    /** Exchange listener: clean inbound requests and restart outbount. */
    private final PartitionsExchangeAware exchAwareLsnr = new PartitionsExchangeAware() {
        @Override public void onDoneAfterTopologyUnlock(GridDhtPartitionsExchangeFuture fut) {

            // Skip join/left client nodes.
            if (fut.exchangeType() != GridDhtPartitionsExchangeFuture.ExchangeType.ALL ||
                cluster.clusterState().lastState() != ClusterState.ACTIVE)
                return;

            DiscoveryEvent evt = fut.firstEvent();

            // Skip create/destroy caches.
            if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT) {
                DiscoveryCustomMessage msg = ((DiscoveryCustomEvent)evt).customMessage();

                if (msg instanceof DynamicCacheChangeBatch)
                    return;

                // Just clear all activities and update topology version.
                if (log.isDebugEnabled())
                    log.debug("Resetting all global statistics activities due to new topology " +
                        fut.topologyVersion());

                inLocalRequests.clear();
                inGloblaRequests.clear();

                Set<StatisticsKey> curColls = curCollections.keySet();

                for (StatisticsKey key : curColls) {
                    curCollections.remove(key);

                    mgmtPool.submit(() -> collectGlobalStatistics(key));
                }

                Set<StatisticsKey> outReqs = outGlobalStatisticsRequests.keySet();

                for (StatisticsKey key : outReqs) {
                    outGlobalStatisticsRequests.remove(key);

                    mgmtPool.submit(() -> collectGlobalStatistics(key));
                }
            }
        }
    };

    /**
     * Constructor.
     *
     * @param statMgr Statistics manager.
     * @param sysViewMgr System view manager.
     * @param mgmtPool Statistics management pool.
     * @param discoMgr Grid discovery manager.
     * @param cluster Cluster state processor.
     * @param exchange Partition exchange manager.
     * @param helper Statistics helper.
     * @param ioMgr Communication manager.
     * @param logSupplier Log supplier.
     */
    public IgniteGlobalStatisticsManager(
        IgniteStatisticsManagerImpl statMgr,
        GridSystemViewManager sysViewMgr,
        IgniteThreadPoolExecutor mgmtPool,
        GridDiscoveryManager discoMgr,
        GridClusterStateProcessor cluster,
        GridCachePartitionExchangeManager<?, ?> exchange,
        IgniteStatisticsHelper helper,
        GridIoManager ioMgr,
        Function<Class<?>, IgniteLogger> logSupplier
    ) {
        this.statMgr = statMgr;
        this.mgmtPool = mgmtPool;
        this.discoMgr = discoMgr;
        this.cluster = cluster;
        this.exchange = exchange;
        this.helper = helper;
        this.ioMgr = ioMgr;
        log = logSupplier.apply(IgniteGlobalStatisticsManager.class);

        statMgr.subscribeToLocalStatistics(nls -> onLocalStatisticsAggregated(nls.key(), nls.statistics(),
            nls.topologyVersion()));
        statMgr.subscribeToStatisticsConfig(this::onConfigChanged);
        ioMgr.addMessageListener(GridTopic.TOPIC_STATISTICS, this);

        sysViewMgr.registerFiltrableView(STAT_GLOBAL_VIEW_NAME, STAT_GLOBAL_VIEW_DESCRIPTION,
            new StatisticsColumnGlobalDataViewWalker(), this::columnGlobalStatisticsViewSupplier, Function.identity());
    }

    /**
     * Statistics column global data view filterable supplier.
     *
     * @param filter Filter.
     * @return Iterable with statistics column global data views.
     */
    private Iterable<StatisticsColumnGlobalDataView> columnGlobalStatisticsViewSupplier(Map<String, Object> filter) {
        String type = (String)filter.get(StatisticsColumnPartitionDataViewWalker.TYPE_FILTER);
        if (type != null && !StatisticsColumnConfigurationView.TABLE_TYPE.equalsIgnoreCase(type))
            return Collections.emptyList();

        String schema = (String)filter.get(StatisticsColumnLocalDataViewWalker.SCHEMA_FILTER);
        String name = (String)filter.get(StatisticsColumnLocalDataViewWalker.NAME_FILTER);
        String column = (String)filter.get(StatisticsColumnPartitionDataViewWalker.COLUMN_FILTER);

        Map<StatisticsKey, ObjectStatisticsImpl> globalStatsMap;
        if (!F.isEmpty(schema) && !F.isEmpty(name)) {
            StatisticsKey key = new StatisticsKey(schema, name);

            CacheEntry<ObjectStatisticsImpl> objLocStat = globalStatistics.get(key);

            if (objLocStat == null || objLocStat.obj == null)
                return Collections.emptyList();

            globalStatsMap = Collections.singletonMap(key, objLocStat.object());
        }
        else
            globalStatsMap = globalStatistics.entrySet().stream()
                .filter(e -> e.getValue().object() != null && (F.isEmpty(schema) || schema.equals(e.getKey().schema())))
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().object()));

        List<StatisticsColumnGlobalDataView> res = new ArrayList<>();

        for (Map.Entry<StatisticsKey, ObjectStatisticsImpl> localStatsEntry : globalStatsMap.entrySet()) {
            StatisticsKey key = localStatsEntry.getKey();
            ObjectStatisticsImpl stat = localStatsEntry.getValue();

            if (column == null) {
                for (Map.Entry<String, ColumnStatistics> colStat : localStatsEntry.getValue().columnsStatistics()
                    .entrySet()) {
                    StatisticsColumnGlobalDataView colStatView = new StatisticsColumnGlobalDataView(key,
                        colStat.getKey(), stat);

                    res.add(colStatView);
                }
            }
            else {
                ColumnStatistics colStat = localStatsEntry.getValue().columnStatistics(column);

                if (colStat != null) {
                    StatisticsColumnGlobalDataView colStatView = new StatisticsColumnGlobalDataView(key, column, stat);

                    res.add(colStatView);
                }
            }
        }

        return res;
    }

    /**
     * Start operations.
     * Shouldn't be called twice.
     */
    public synchronized void start() {
        if (started) {
            if (log.isDebugEnabled())
                log.debug("IgniteGlobalStatisticsManager already started.");

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Global statistics manager starting...");

        globalStatistics.clear();
        exchange.registerExchangeAwareComponent(exchAwareLsnr);

        started = true;

        if (log.isDebugEnabled())
            log.debug("Global statistics manager started.");
    }

    /**
     * Stop operations.
     * Shouldn't be called twice.
     */
    public synchronized void stop() {
        if (!started) {
            if (log.isDebugEnabled())
                log.debug("IgniteGlobalStatisticsManager already stopped.");

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Global statistics manager stopping...");

        globalStatistics.clear();

        inGloblaRequests.clear();
        inLocalRequests.clear();
        outGlobalStatisticsRequests.clear();
        curCollections.clear();

        exchange.unregisterExchangeAwareComponent(exchAwareLsnr);

        started = false;

        if (log.isDebugEnabled())
            log.debug("Global statistics manager stopped.");
    }

    /**
     * Get global statistics for the given key. If there is no cached statistics, but there is cache record with
     * empty object - no additional collection will be started.
     *
     * @param key Statistics key.
     * @return Global object statistics or {@code null} if there is no global statistics available.
     */
    public ObjectStatisticsImpl getGlobalStatistics(StatisticsKey key) {
        CacheEntry<ObjectStatisticsImpl> res = globalStatistics.computeIfAbsent(key, k -> {
            if (log.isDebugEnabled())
                log.debug("Scheduling global statistics collection by key " + key);

            mgmtPool.submit(() -> collectGlobalStatistics(key));

            return new CacheEntry<>(null);
        });

        return res.object();
    }

    /**
     * Either send local or global statistics request to get global statistics.
     *
     * @param key Statistics key to get global statistics by.
     */
    private void collectGlobalStatistics(StatisticsKey key) {
        try {
            StatisticsObjectConfiguration statCfg = statMgr.statisticConfiguration().config(key);

            if (statCfg != null && !statCfg.columns().isEmpty()) {
                UUID statMaster = getStatisticsMasterNode(key);

                if (discoMgr.localNode().id().equals(statMaster))
                    gatherGlobalStatistics(statCfg);
                else {
                    StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(),
                        Collections.emptyList());

                    Map<String, Long> versions = statCfg.columns().entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().version()));

                    StatisticsRequest globalReq = new StatisticsRequest(UUID.randomUUID(), keyMsg,
                        StatisticsType.GLOBAL, null, versions);

                    outGlobalStatisticsRequests.put(key, globalReq.reqId());

                    if (log.isDebugEnabled())
                        log.debug("Send global statistics request by configuration " + statCfg);

                    send(statMaster, globalReq);

                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Unable to start global statistics collection due to lack of configuration by key "
                        + key);
            }

        }
        catch (IgniteCheckedException e) {
            if (log.isInfoEnabled())
                log.info("Unable to get statistics configuration due to " + e.getMessage());
        }
    }

    /**
     * Collect global statistics on master node.
     *
     * @param statCfg Statistics config to gather global statistics by.
     */
    private void gatherGlobalStatistics(StatisticsObjectConfiguration statCfg) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Start global statistics collection by configuration " + statCfg);

        StatisticsTarget target = new StatisticsTarget(statCfg.key());

        List<StatisticsAddressedRequest> locRequests = helper.generateGatheringRequests(target, statCfg);
        UUID reqId = locRequests.get(0).req().reqId();

        StatisticsGatheringContext gatCtx = new StatisticsGatheringContext(locRequests.size(), reqId, statCfg);

        curCollections.put(statCfg.key(), gatCtx);

        for (StatisticsAddressedRequest addReq : locRequests) {
            if (log.isDebugEnabled())
                log.debug("Sending local request " + addReq.req().reqId() + " to node " + addReq.nodeId());

            send(addReq.nodeId(), addReq.req());
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
        mgmtPool.submit(() -> {
            try {
                if (msg instanceof StatisticsRequest) {
                    StatisticsRequest req = (StatisticsRequest)msg;
                    switch (req.type()) {
                        case LOCAL:
                            processLocalRequest(nodeId, req);

                            break;

                        case GLOBAL:
                            processGlobalRequest(nodeId, req);

                            break;

                        default:
                            log.warning("Unexpected type " + req.type() + " in statistics request message " + req);
                    }
                }
                else if (msg instanceof StatisticsResponse) {
                    StatisticsResponse resp = (StatisticsResponse)msg;

                    switch (resp.data().type()) {
                        case LOCAL:
                            processLocalResponse(nodeId, resp);

                            break;

                        case GLOBAL:
                            processGlobalResponse(nodeId, resp);

                            break;

                        default:
                            log.warning("Unexpected type " + resp.data().type() +
                                " in statistics reposonse message " + resp);
                    }

                }
                else
                    log.warning("Unknown msg " + msg + " in statistics topic " + GridTopic.TOPIC_STATISTICS +
                        " from node " + nodeId);
            }
            catch (Throwable e) {
                log.warning("Unable to process statistics message: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Process request for local statistics.
     * 1) If there are local statistics for the given key - send response.
     * 2) If there is no such statistics - add request to incoming queue.
     *
     * @param nodeId Sender node id.
     * @param req Request to process.
     * @throws IgniteCheckedException
     */
    private void processLocalRequest(UUID nodeId, StatisticsRequest req) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Got local statistics request from node " + nodeId + " : " + req);

        StatisticsKey key = new StatisticsKey(req.key().schema(), req.key().obj());

        ObjectStatisticsImpl objectStatistics = statMgr.getLocalStatistics(key, req.topVer());

        if (StatisticsUtils.compareVersions(objectStatistics, req.versions()) == 0)
            sendResponse(nodeId, req.reqId(), key, StatisticsType.LOCAL, objectStatistics);
        else {
            addToRequests(inLocalRequests, key, new StatisticsAddressedRequest(req, nodeId));

            objectStatistics = statMgr.getLocalStatistics(key, req.topVer());

            if (StatisticsUtils.compareVersions(objectStatistics, req.versions()) == 0) {
                StatisticsAddressedRequest removed = removeFromRequests(inLocalRequests, key, req.reqId());

                if (removed != null)
                    sendResponse(nodeId, req.reqId(), key, StatisticsType.LOCAL, objectStatistics);
            }
        }
    }

    /**
     * Test if statistics configuration is fit to all required versions.
     * @param cfg Statistics configuration to check.
     * @param versions Map of column name to required version.
     * @return {@code true} if it is, {@code false} otherwise.
     */
    private boolean checkStatisticsCfg(StatisticsObjectConfiguration cfg, Map<String, Long> versions) {
        if (cfg == null)
            return false;

        for (Map.Entry<String, Long> version : versions.entrySet()) {
            StatisticsColumnConfiguration colCfg = cfg.columns().get(version.getKey());

            if (colCfg == null || colCfg.version() < version.getValue())
                return false;
        }

        return true;
    }

    /**
     * Process incoming request for global statistics. Either response (if it exists), or collect and response
     * (if current node is master node for the given key) or ignore (if current node is no more master node for
     * the given key.
     *
     * @param nodeId Sender node id.
     * @param req Request.
     */
    private void processGlobalRequest(UUID nodeId, StatisticsRequest req) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Got global statistics request from node " + nodeId + " : " + req);

        StatisticsKey key = new StatisticsKey(req.key().schema(), req.key().obj());

        ObjectStatisticsImpl objStatistics = getGlobalStatistics(key, req.versions());

        if (objStatistics == null) {
            if (discoMgr.localNode().id().equals(getStatisticsMasterNode(key))) {

                addToRequests(inGloblaRequests, key, new StatisticsAddressedRequest(req, nodeId));
                globalStatistics.computeIfAbsent(key, k -> new CacheEntry<>(null));

                if (!hasCurrentCollection(key, req.versions())) {
                    StatisticsObjectConfiguration objCfg = statMgr.statisticConfiguration().config(key);

                    if (StatisticsUtils.compareVersions(objCfg, req.versions()) >= 0)
                        gatherGlobalStatistics(objCfg);
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Wait for statistics configuration to process global statistics request " +
                                req.reqId());
                    }
                }
            }

            objStatistics = getGlobalStatistics(key, req.versions());

            if (objStatistics != null) {
                StatisticsAddressedRequest removed = removeFromRequests(inGloblaRequests, key, req.reqId());

                if (removed != null)
                    sendResponse(nodeId, req.reqId(), key, StatisticsType.GLOBAL, objStatistics);
            }
        }
        else
            sendResponse(nodeId, req.reqId(), key, StatisticsType.GLOBAL, objStatistics);
    }

    /**
     * Check if there are already started current collection with specified parameters.
     *
     * @param key Statistics key.
     * @param versions Reuqired versions.
     * @return {@code true} if there are already current collection with specifie parameters, {@code false} - otherwise.
     */
    private boolean hasCurrentCollection(StatisticsKey key, Map<String, Long> versions) {
        StatisticsGatheringContext ctx = curCollections.get(key);

        if (ctx == null)
            return false;

        return StatisticsUtils.compareVersions(ctx.configuration(), versions) == 0;
    }

    /**
     * Get apptopriate global statistics from cache.
     *
     * @param key Statistics key.
     * @param versions Required versions.
     * @return Global statistics or {@code null} if there are no such global statistics.
     */
    private ObjectStatisticsImpl getGlobalStatistics(StatisticsKey key, Map<String, Long> versions) {
        CacheEntry<ObjectStatisticsImpl> objStatEntry = globalStatistics.get(key);

        if (objStatEntry == null || StatisticsUtils.compareVersions(objStatEntry.object(), versions) != 0)
            return null;

        return objStatEntry.object();
    }

    /**
     * Build statistics response and send it to specified node.
     *
     * @param nodeId Target node id.
     * @param reqId Request id.
     * @param key Statistics key.
     * @param type Statistics type.
     * @param data Statitsics data.
     */
    private void sendResponse(
        UUID nodeId,
        UUID reqId,
        StatisticsKey key,
        StatisticsType type,
        ObjectStatisticsImpl data
    ) throws IgniteCheckedException {
        StatisticsKeyMessage keyMsg = new StatisticsKeyMessage(key.schema(), key.obj(), null);
        StatisticsObjectData dataMsg = StatisticsUtils.toObjectData(keyMsg, type, data);

        send(nodeId, new StatisticsResponse(reqId, dataMsg));
    }

    /**
     * Add to addressed requests map.
     *
     * @param map Map to add into.
     * @param key Request statistics key.
     * @param req Request to add.
     */
    private void addToRequests(
        ConcurrentMap<StatisticsKey, Collection<StatisticsAddressedRequest>> map,
        StatisticsKey key,
        StatisticsAddressedRequest req
    ) {
        map.compute(key, (k, v) -> {
            if (v == null)
                v = new ArrayList<>();

            v.add(req);

            return v;
        });
    }

    /**
     * Check if specified map contains request with specified key and id, remove and return it.
     *
     * @param key Request statistics key.
     * @param reqId Request id.
     * @return Removed request.
     */
    private StatisticsAddressedRequest removeFromRequests(
        ConcurrentMap<StatisticsKey, Collection<StatisticsAddressedRequest>> map,
        StatisticsKey key,
        UUID reqId
    ) {
        StatisticsAddressedRequest[] res = new StatisticsAddressedRequest[1];

        map.compute(key, (k, v) -> {
            if (v != null)
                res[0] = v.stream().filter(e -> reqId.equals(e.req().reqId())).findAny().orElse(null);

            if (res[0] != null)
                v = v.stream().filter(e -> !reqId.equals(e.req().reqId())).collect(Collectors.toList());

            return v;
        });

        return res[0];
    }

    /**
     * Process statistics configuration changes:
     *
     * 1) Remove all outbound activity by specified key, inbound may be suspended due to lack of
     *  requested configuration.
     * 2) Remove all inbound activity by changed key if reqiest col cfg versions lower than configuration col cfg versions.
     * 3.1) If there are no live column's config - remove cached global statistics.
     * 3.2) If there are some live columns config and global statistics cache contains statistics for the given key -
     * start to collect it again.
     */
    public void onConfigChanged(StatisticsObjectConfiguration cfg) {
        StatisticsKey key = cfg.key();

        curCollections.remove(key);
        outGlobalStatisticsRequests.remove(key);

        inLocalRequests.computeIfPresent(key, (k, v) -> {
            // Current config newer than income request - request should be invalidated.
            v.removeIf(req -> StatisticsUtils.compareVersions(cfg, req.req().versions()) > 0);

            return (v.isEmpty()) ? null : v;
        });

        inGloblaRequests.computeIfPresent(key, (k, v) -> {
            v.removeIf(req -> StatisticsUtils.compareVersions(cfg, req.req().versions()) > 0);

            return (v.isEmpty()) ? null : v;
        });

        if (cfg.columns().isEmpty())
            globalStatistics.remove(key);
        else {
            globalStatistics.computeIfPresent(key, (k, v) -> {
                if (v != null) {
                    if (log.isDebugEnabled())
                        log.debug("Scheduling global statistics recollection by key " + key);

                    mgmtPool.submit(() -> collectGlobalStatistics(key));
                }
                return v;
            });
        }
    }

    /**
     * Clear global object statistics.
     *
     * @param key Object key to clear global statistics by.
     * @param colNames Only statistics by specified columns will be cleared.
     */
    public void clearGlobalStatistics(StatisticsKey key, Set<String> colNames) {
        globalStatistics.computeIfPresent(key, (k, v) -> {
            ObjectStatisticsImpl globStatOld = v.object();
            ObjectStatisticsImpl globStatNew = (globStatOld == null) ? null : globStatOld.subtract(colNames);

            return (globStatNew == null || globStatNew.columnsStatistics().isEmpty()) ? null :
                new CacheEntry<>(globStatNew);
        });

        outGlobalStatisticsRequests.remove(key);
    }

    /**
     * Process response with local statistics. Try to finish collecting operation and send pending requests.
     *
     * @param nodeId Sender node id.
     * @param resp Statistics response to process.
     * @throws IgniteCheckedException In case of error.
     */
    private void processLocalResponse(UUID nodeId, StatisticsResponse resp) throws IgniteCheckedException {
        StatisticsKeyMessage keyMsg = resp.data().key();
        StatisticsKey key = new StatisticsKey(keyMsg.schema(), resp.data().key().obj());

        if (log.isDebugEnabled())
            log.debug("Got local statistics response " + resp.reqId() + " from node " + nodeId + " by key " + key);

        StatisticsGatheringContext curCtx = curCollections.get(key);

        if (curCtx != null) {
            if (!curCtx.reqId().equals(resp.reqId())) {
                if (log.isDebugEnabled())
                    log.debug("Got outdated local statistics response " + resp + " instead of " + curCtx.reqId());

                return;
            }

            ObjectStatisticsImpl data = StatisticsUtils.toObjectStatistics(null, resp.data());

            if (curCtx.registerResponse(data)) {
                StatisticsObjectConfiguration cfg = statMgr.statisticConfiguration().config(key);

                if (cfg != null) {
                    if (log.isDebugEnabled())
                        log.debug("Aggregating global statistics for key " + key + " by request " + curCtx.reqId());

                    ObjectStatisticsImpl globalStat = helper.aggregateLocalStatistics(cfg, curCtx.collectedData());

                    globalStatistics.put(key, new CacheEntry<>(globalStat));

                    if (log.isDebugEnabled())
                        log.debug("Global statistics for key " + key + " collected.");

                    Collection<StatisticsAddressedRequest> globalRequests = inGloblaRequests.remove(key);

                    if (globalRequests != null) {
                        StatisticsObjectData globalStatData = StatisticsUtils.toObjectData(keyMsg,
                            StatisticsType.GLOBAL, globalStat);

                        for (StatisticsAddressedRequest req : globalRequests) {
                            StatisticsResponse outResp = new StatisticsResponse(req.req().reqId(), globalStatData);

                            send(req.nodeId(), outResp);
                        }
                    }
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Dropping collected statistics due to lack of configuration for key " + key);
                }

                curCollections.remove(key);
            }
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Got outdated local statistics response " + resp);
        }
    }

    /**
     * Process response of global statistics.
     *
     * @param resp Response.
     * @throws IgniteCheckedException In case of error.
     */
    private void processGlobalResponse(UUID nodeId, StatisticsResponse resp) throws IgniteCheckedException {
        StatisticsKeyMessage keyMsg = resp.data().key();
        StatisticsKey key = new StatisticsKey(keyMsg.schema(), keyMsg.obj());

        if (log.isDebugEnabled())
            log.debug("Got global statistics response " + resp.reqId() + " from node " + nodeId + " by key " + key);

        UUID reqId = outGlobalStatisticsRequests.get(key);

        if (reqId != null) {
            if (!resp.reqId().equals(reqId)) {
                if (log.isDebugEnabled())
                    log.debug("Got outdated global statistics response " + resp + " instead of " + reqId);

                return;
            }

            ObjectStatisticsImpl data = StatisticsUtils.toObjectStatistics(null, resp.data());

            globalStatistics.put(key, new CacheEntry(data));
            outGlobalStatisticsRequests.remove(key);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Got outdated global statistics response " + resp);
        }
    }

    /**
     * Calculate id of statistics master node for the given key.
     *
     * @param key Statistics key to calculate master node for.
     * @return UUID of statistics master node.
     */
    private UUID getStatisticsMasterNode(StatisticsKey key) {
        UUID[] nodes = discoMgr.aliveServerNodes().stream().map(ClusterNode::id).sorted().toArray(UUID[]::new);
        int idx = IgniteUtils.hashToIndex(key.obj().hashCode(), nodes.length);

        return nodes[idx];
    }

    /**
     * After collecting local statistics - check if there are some pending request for it and send responces.
     *
     * @param key Statistics key on which local statistics was aggregated.
     * @param statistics Collected statistics by key.
     * @param topVer Topology version which aggregated statistics stands for.
     */
    public void onLocalStatisticsAggregated(
        StatisticsKey key,
        ObjectStatisticsImpl statistics,
        AffinityTopologyVersion topVer
    ) {
        List<StatisticsAddressedRequest> inReqs = new ArrayList<>();

        inLocalRequests.computeIfPresent(key, (k, v) -> {
            List<StatisticsAddressedRequest> left = new ArrayList<>();

            for (StatisticsAddressedRequest req : v) {
                if (topVer.equals(req.req().topVer()) &&
                    StatisticsUtils.compareVersions(statistics, req.req().versions()) == 0)
                    inReqs.add(req);
                else
                    left.add(req);
            }

            return (left.isEmpty()) ? null : left;
        });

        if (inReqs.isEmpty())
            return;

        for (StatisticsAddressedRequest req : inReqs) {
            try {
                sendResponse(req.nodeId(), req.req().reqId(), key, StatisticsType.LOCAL, statistics);
            }
            catch (IgniteCheckedException e) {
                log.info("Unable to send local object statistics for key " + key + " due to " + e.getMessage());
            }
        }
    }

    /**
     * Send statistics related message.
     *
     * @param nodeId Target node id.
     * @param msg Message to send.
     * @throws IgniteCheckedException In case of error.
     */
    private void send(UUID nodeId, StatisticsRequest msg) throws IgniteCheckedException {
        if (discoMgr.localNode().id().equals(nodeId)) {
            switch (msg.type()) {
                case LOCAL:
                    processLocalRequest(nodeId, msg);

                    break;

                default:
                    log.warning("Unexpected type " + msg.type() + " in statistics request message " + msg);
            }
        }
        else
            ioMgr.sendToGridTopic(nodeId, GridTopic.TOPIC_STATISTICS, msg, GridIoPolicy.MANAGEMENT_POOL);
    }

    /**
     * Send statistics response or process it locally.
     *
     * @param nodeId Target node id. If equals to local node - corresponding method will be called directly.
     * @param msg Statistics response to send.
     * @throws IgniteCheckedException In case of error.
     */
    private void send(UUID nodeId, StatisticsResponse msg) throws IgniteCheckedException {
        if (discoMgr.localNode().id().equals(nodeId)) {
            switch (msg.data().type()) {
                case LOCAL:
                    processLocalResponse(nodeId, msg);

                    break;

                case GLOBAL:
                    processGlobalResponse(nodeId, msg);

                    break;

                default:
                    log.warning("Unexpected type " + msg.data().type() + " in statistics response message " + msg);
            }
        }
        else
            ioMgr.sendToGridTopic(nodeId, GridTopic.TOPIC_STATISTICS, msg, GridIoPolicy.MANAGEMENT_POOL);
    }

    /** Cache entry. */
    private static class CacheEntry<T> {
        /** Cached object. */
        private final T obj;

        /**
         * Constructor.
         *
         * @param obj Cached object.
         */
        public CacheEntry(T obj) {
            this.obj = obj;
        }

        /**
         * @return Cached object.
         */
        public T object() {
            return obj;
        }
    }

    /** Context of global statistics gathering. */
    private static class StatisticsGatheringContext {
        /** Number of remaining requests. */
        private int remainingResponses;

        /** Requests id. */
        private final UUID reqId;

        /** Local object statistics from responses. */
        private final Collection<ObjectStatisticsImpl> responses = new ArrayList<>();

        /** Configuration, used to collect statistics. */
        private final StatisticsObjectConfiguration cfg;

        /**
         * Constructor.
         *
         * @param responseCont Expectiong response count.
         * @param reqId Requests id.
         * @param cfg Configuration, used to collect statistics.
         */
        public StatisticsGatheringContext(int responseCont, UUID reqId, StatisticsObjectConfiguration cfg) {
            remainingResponses = responseCont;
            this.reqId = reqId;
            this.cfg = cfg;
        }

        /**
         * Register response.
         *
         * @param data Object statistics from response.
         * @return {@code true} if all respones collected, {@code false} otherwise.
         */
        public synchronized boolean registerResponse(ObjectStatisticsImpl data) {
            responses.add(data);
            return --remainingResponses == 0;
        }

        /**
         * @return Requests id.
         */
        public UUID reqId() {
            return reqId;
        }

        /**
         * Get collected local object statistics.
         * @return Local object statistics.
         */
        public Collection<ObjectStatisticsImpl> collectedData() {
            assert remainingResponses == 0;

            return responses;
        }

        /**
         * @return Statistics configuration, used to start gathering.
         */
        public StatisticsObjectConfiguration configuration() {
            return cfg;
        }
    }
}
