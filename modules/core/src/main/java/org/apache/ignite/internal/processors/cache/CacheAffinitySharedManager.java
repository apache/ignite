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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.ClientCacheDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CacheGroupAffinityMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cluster.DiscoveryDataClusterState;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteInClosureX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 *
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class CacheAffinitySharedManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** */
    private final long clientCacheMsgTimeout =
        IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_CLIENT_CACHE_CHANGE_MESSAGE_TIMEOUT, 10_000);

    /** */
    private static final IgniteClosure<ClusterNode, UUID> NODE_TO_ID = new IgniteClosure<ClusterNode, UUID>() {
        @Override public UUID apply(ClusterNode node) {
            return node.id();
        }
    };

    /** */
    private static final IgniteClosure<ClusterNode, Long> NODE_TO_ORDER = new IgniteClosure<ClusterNode, Long>() {
        @Override public Long apply(ClusterNode node) {
            return node.order();
        }
    };

    /** Affinity information for all started caches (initialized on coordinator). */
    private ConcurrentMap<Integer, CacheGroupHolder> grpHolders = new ConcurrentHashMap<>();

    /** Topology version which requires affinity re-calculation (set from discovery thread). */
    private AffinityTopologyVersion lastAffVer;

    /** Registered caches (updated from exchange thread). */
    private final CachesInfo caches = new CachesInfo();

    /** */
    private WaitRebalanceInfo waitInfo;

    /** */
    private final Object mux = new Object();

    /** Pending affinity assignment futures. */
    private final ConcurrentMap<Long, GridDhtAssignmentFetchFuture> pendingAssignmentFetchFuts =
        new ConcurrentHashMap<>();

    /** */
    private final ThreadLocal<ClientCacheChangeDiscoveryMessage> clientCacheChanges = new ThreadLocal<>();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            DiscoveryEvent e = (DiscoveryEvent)evt;

            assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

            ClusterNode n = e.eventNode();

            for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                fut.onNodeLeft(n.id());
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        cctx.kernalContext().event().addLocalEventListener(discoLsnr, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /**
     * Callback invoked from discovery thread when discovery message is received.
     *
     * @param type Event type.
     * @param customMsg Custom message instance.
     * @param node Event node.
     * @param topVer Topology version.
     * @param state Cluster state.
     */
    void onDiscoveryEvent(int type,
        @Nullable DiscoveryCustomMessage customMsg,
        ClusterNode node,
        AffinityTopologyVersion topVer,
        DiscoveryDataClusterState state) {
        if (type == EVT_NODE_JOINED && node.isLocal())
            lastAffVer = null;

        if ((state.transition() || !state.active()) &&
            !DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(customMsg))
            return;

        if ((!node.isClient() && (type == EVT_NODE_FAILED || type == EVT_NODE_JOINED || type == EVT_NODE_LEFT)) ||
            DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(customMsg)) {
            synchronized (mux) {
                assert lastAffVer == null || topVer.compareTo(lastAffVer) > 0 :
                    "lastAffVer=" + lastAffVer + ", topVer=" + topVer + ", customMsg=" + customMsg;

                lastAffVer = topVer;
            }
        }
    }

    /**
     * Must be called from exchange thread.
     */
    public void initCachesOnLocalJoin(
        Map<Integer, CacheGroupDescriptor> cacheGroupDescriptors,
        Map<String, DynamicCacheDescriptor> cacheDescriptors
    ) {
        // Clean-up in case of client reconnect.
        caches.clear();

        caches.init(cacheGroupDescriptors, cacheDescriptors);
    }

    /**
     * Callback invoked from discovery thread when discovery custom message is received.
     *
     * @param msg Customer message.
     * @return {@code True} if minor topology version should be increased.
     */
    boolean onCustomEvent(CacheAffinityChangeMessage msg) {
        if (msg.exchangeId() != null) {
            if (log.isDebugEnabled()) {
                log.debug("Ignore affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() + ']');
            }

            return false;
        }

        // Skip message if affinity was already recalculated.
        boolean exchangeNeeded = lastAffVer == null || lastAffVer.equals(msg.topologyVersion());

        msg.exchangeNeeded(exchangeNeeded);

        if (exchangeNeeded) {
            if (log.isDebugEnabled()) {
                log.debug("Need process affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() + ']');
            }
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Ignore affinity change message [lastAffVer=" + lastAffVer +
                    ", msgExchId=" + msg.exchangeId() +
                    ", msgVer=" + msg.topologyVersion() + ']');
            }
        }

        return exchangeNeeded;
    }

    /**
     * @param topVer Expected topology version.
     */
    private void onCacheGroupStopped(AffinityTopologyVersion topVer) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null || !waitInfo.topVer.equals(topVer))
                return;

            if (waitInfo.waitGrps.isEmpty()) {
                msg = affinityChangeMessage(waitInfo);

                waitInfo = null;
            }
        }

        try {
            if (msg != null)
                cctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send affinity change message.", e);
        }
    }

    /**
     * @param top Topology.
     * @param checkGrpId Group ID.
     */
    void checkRebalanceState(GridDhtPartitionTopology top, Integer checkGrpId) {
        CacheAffinityChangeMessage msg = null;

        synchronized (mux) {
            if (waitInfo == null || !waitInfo.topVer.equals(lastAffVer) )
                return;

            Map<Integer, UUID> partWait = waitInfo.waitGrps.get(checkGrpId);

            boolean rebalanced = true;

            if (partWait != null) {
                CacheGroupHolder grpHolder = grpHolders.get(checkGrpId);

                if (grpHolder != null) {
                    for (Iterator<Map.Entry<Integer, UUID>> it = partWait.entrySet().iterator(); it.hasNext(); ) {
                        Map.Entry<Integer, UUID> e = it.next();

                        Integer part = e.getKey();
                        UUID waitNode = e.getValue();

                        GridDhtPartitionState state = top.partitionState(waitNode, part);

                        if (state != GridDhtPartitionState.OWNING) {
                            rebalanced = false;

                            break;
                        }
                        else
                            it.remove();
                    }
                }

                if (rebalanced) {
                    waitInfo.waitGrps.remove(checkGrpId);

                    if (waitInfo.waitGrps.isEmpty()) {
                        msg = affinityChangeMessage(waitInfo);

                        waitInfo = null;
                    }
                }
            }

            try {
                if (msg != null)
                    cctx.discovery().sendCustomEvent(msg);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send affinity change message.", e);
            }
        }
    }

    /**
     * @return Group IDs.
     */
    public Set<Integer> waitGroups() {
        synchronized (mux) {
            if (waitInfo == null || !waitInfo.topVer.equals(lastAffVer) )
                return Collections.emptySet();

            return new HashSet<>(waitInfo.waitGrps.keySet());
        }
    }

    /**
     * @param waitInfo Cache rebalance information.
     * @return Message.
     */
    @Nullable private CacheAffinityChangeMessage affinityChangeMessage(WaitRebalanceInfo waitInfo) {
        if (waitInfo.assignments.isEmpty()) // Possible if all awaited caches were destroyed.
            return null;

        Map<Integer, Map<Integer, List<UUID>>> assignmentsChange = U.newHashMap(waitInfo.assignments.size());

        for (Map.Entry<Integer, Map<Integer, List<ClusterNode>>> e : waitInfo.assignments.entrySet()) {
            Integer grpId = e.getKey();

            Map<Integer, List<ClusterNode>> assignment = e.getValue();

            Map<Integer, List<UUID>> assignment0 = U.newHashMap(assignment.size());

            for (Map.Entry<Integer, List<ClusterNode>> e0 : assignment.entrySet())
                assignment0.put(e0.getKey(), toIds0(e0.getValue()));

            assignmentsChange.put(grpId, assignment0);
        }

        return new CacheAffinityChangeMessage(waitInfo.topVer, assignmentsChange, waitInfo.deploymentIds);
    }

    /**
     * @param grp Cache group.
     */
    void onCacheGroupCreated(CacheGroupContext grp) {
        if (!grpHolders.containsKey(grp.groupId())) {
            cctx.io().addCacheGroupHandler(grp.groupId(), GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(nodeId, res);
                    }
                });
        }
    }

    /**
     * @param reqId Request ID.
     * @param startReqs Client cache start request.
     * @return Descriptors for caches to start.
     */
    @Nullable private List<DynamicCacheDescriptor> clientCachesToStart(UUID reqId,
        Map<String, DynamicCacheChangeRequest> startReqs) {
        List<DynamicCacheDescriptor> startDescs = new ArrayList<>(startReqs.size());

        for (DynamicCacheChangeRequest startReq : startReqs.values()) {
            DynamicCacheDescriptor desc = caches.cache(CU.cacheId(startReq.cacheName()));

            if (desc == null) {
                CacheException err = new CacheException("Failed to start client cache " +
                    "(a cache with the given name is not started): " + startReq.cacheName());

                cctx.cache().completeClientCacheChangeFuture(reqId, err);

                return null;
            }

            if (cctx.cacheContext(desc.cacheId()) != null)
                continue;

            startDescs.add(desc);
        }

        return startDescs;
    }

    /**
     * @param msg Change request.
     * @param crd Coordinator flag.
     * @param topVer Current topology version.
     * @param discoCache Discovery data cache.
     * @return Map of started caches (cache ID to near enabled flag).
     */
    @Nullable private Map<Integer, Boolean> processClientCacheStartRequests(
        ClientCacheChangeDummyDiscoveryMessage msg,
        boolean crd,
        AffinityTopologyVersion topVer,
        DiscoCache discoCache) {
        Map<String, DynamicCacheChangeRequest> startReqs = msg.startRequests();

        if (startReqs == null)
            return null;

        List<DynamicCacheDescriptor> startDescs = clientCachesToStart(msg.requestId(), msg.startRequests());

        if (startDescs == null || startDescs.isEmpty()) {
            cctx.cache().completeClientCacheChangeFuture(msg.requestId(), null);

            return null;
        }

        Map<Integer, GridDhtAssignmentFetchFuture> fetchFuts = U.newHashMap(startDescs.size());

        Set<String> startedCaches = U.newHashSet(startDescs.size());

        Map<Integer, Boolean> startedInfos = U.newHashMap(startDescs.size());

        for (DynamicCacheDescriptor desc : startDescs) {
            try {
                startedCaches.add(desc.cacheName());

                DynamicCacheChangeRequest startReq = startReqs.get(desc.cacheName());

                cctx.cache().prepareCacheStart(desc.cacheConfiguration(),
                    desc,
                    startReq.nearCacheConfiguration(),
                    topVer,
                    startReq.disabledAfterStart());

                startedInfos.put(desc.cacheId(), startReq.nearCacheConfiguration() != null);

                CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                assert grp != null : desc.groupId();
                assert !grp.affinityNode() || grp.isLocal() : grp.cacheOrGroupName();

                if (!grp.isLocal() && grp.affinity().lastVersion().equals(AffinityTopologyVersion.NONE)) {
                    assert grp.localStartVersion().equals(topVer) : grp.localStartVersion();

                    if (crd) {
                        CacheGroupHolder grpHolder = grpHolders.get(grp.groupId());

                        assert grpHolder != null && grpHolder.affinity().idealAssignment() != null;

                        if (grpHolder.client()) {
                            ClientCacheDhtTopologyFuture topFut = new ClientCacheDhtTopologyFuture(topVer);

                            grp.topology().updateTopologyVersion(topFut, discoCache, -1, false);

                            grpHolder = new CacheGroupHolder1(grp, grpHolder.affinity());

                            grpHolders.put(grp.groupId(), grpHolder);

                            GridClientPartitionTopology clientTop = cctx.exchange().clearClientTopology(grp.groupId());

                            if (clientTop != null) {
                                grp.topology().update(grpHolder.affinity().lastVersion(),
                                    clientTop.partitionMap(true),
                                    clientTop.fullUpdateCounters(),
                                    Collections.<Integer>emptySet(),
                                    null,
                                    null);
                            }

                            assert grpHolder.affinity().lastVersion().equals(grp.affinity().lastVersion());
                        }
                    }
                    else if (!fetchFuts.containsKey(grp.groupId())) {
                        GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                            grp.groupId(),
                            topVer,
                            discoCache);

                        fetchFut.init(true);

                        fetchFuts.put(grp.groupId(), fetchFut);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                cctx.cache().closeCaches(startedCaches, false);

                cctx.cache().completeClientCacheChangeFuture(msg.requestId(), e);

                return null;
            }
        }

        for (GridDhtAssignmentFetchFuture fetchFut : fetchFuts.values()) {
            try {
                CacheGroupContext grp = cctx.cache().cacheGroup(fetchFut.groupId());

                assert grp != null;

                GridDhtAffinityAssignmentResponse res = fetchAffinity(topVer,
                    null,
                    discoCache,
                    grp.affinity(),
                    fetchFut);

                GridDhtPartitionFullMap partMap;
                ClientCacheDhtTopologyFuture topFut;

                if (res != null) {
                    partMap = res.partitionMap();

                    assert partMap != null : res;

                    topFut = new ClientCacheDhtTopologyFuture(topVer);
                }
                else {
                    partMap = new GridDhtPartitionFullMap(cctx.localNodeId(), cctx.localNode().order(), 1);

                    topFut = new ClientCacheDhtTopologyFuture(topVer,
                        new ClusterTopologyServerNotFoundException("All server nodes left grid."));
                }

                grp.topology().updateTopologyVersion(topFut, discoCache, -1, false);

                grp.topology().update(topVer, partMap, null, Collections.<Integer>emptySet(), null, null);

                topFut.validate(grp, discoCache.allNodes());
            }
            catch (IgniteCheckedException e) {
                cctx.cache().closeCaches(startedCaches, false);

                cctx.cache().completeClientCacheChangeFuture(msg.requestId(), e);

                return null;
            }
        }

        for (DynamicCacheDescriptor desc : startDescs) {
            if (desc.cacheConfiguration().getCacheMode() != LOCAL) {
                CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

                assert grp != null;

                grp.topology().onExchangeDone(null, grp.affinity().cachedAffinity(topVer), true);
            }
        }

        cctx.cache().initCacheProxies(topVer, null);

        cctx.cache().completeClientCacheChangeFuture(msg.requestId(), null);

        return startedInfos;
    }

    /**
     * @param msg Change request.
     * @param topVer Current topology version.
     * @param crd Coordinator flag.
     * @return Closed caches IDs.
     */
    private Set<Integer> processCacheCloseRequests(
        ClientCacheChangeDummyDiscoveryMessage msg,
        boolean crd,
        AffinityTopologyVersion topVer) {
        Set<String> cachesToClose = msg.cachesToClose();

        if (cachesToClose == null)
            return null;

        Set<Integer> closed = cctx.cache().closeCaches(cachesToClose, true);

        if (crd) {
            for (CacheGroupHolder hld : grpHolders.values()) {
                if (!hld.client() && cctx.cache().cacheGroup(hld.groupId()) == null) {
                    int grpId = hld.groupId();

                    // All client cache groups were stopped, need create 'client' CacheGroupHolder.
                    CacheGroupHolder grpHolder = grpHolders.remove(grpId);

                    assert grpHolder != null && !grpHolder.client() : grpHolder;

                    try {
                        grpHolder = CacheGroupHolder2.create(cctx,
                            caches.group(grpId),
                            topVer,
                            grpHolder.affinity());

                        grpHolders.put(grpId, grpHolder);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to initialize cache: " + e, e);
                    }
                }
            }
        }

        cctx.cache().completeClientCacheChangeFuture(msg.requestId(), null);

        return closed;
    }

    /**
     * Process client cache start/close requests, called from exchange thread.
     *
     * @param msg Change request.
     */
    void processClientCachesChanges(ClientCacheChangeDummyDiscoveryMessage msg) {
        AffinityTopologyVersion topVer = cctx.exchange().readyAffinityVersion();

        DiscoCache discoCache = cctx.discovery().discoCache(topVer);

        boolean crd = cctx.localNode().equals(discoCache.oldestAliveServerNode());

        Map<Integer, Boolean> startedCaches = processClientCacheStartRequests(msg, crd, topVer, discoCache);

        Set<Integer> closedCaches = processCacheCloseRequests(msg, crd, topVer);

        if (startedCaches != null || closedCaches != null)
            scheduleClientChangeMessage(startedCaches, closedCaches);
    }

    /**
     * Sends discovery message about started/closed client caches, called from exchange thread.
     *
     * @param timeoutObj Timeout object initiated send.
     */
    void sendClientCacheChangesMessage(ClientCacheUpdateTimeout timeoutObj) {
        ClientCacheChangeDiscoveryMessage msg = clientCacheChanges.get();

        // Timeout object was changed if one more client cache changed during timeout,
        // another timeoutObj was scheduled.
        if (msg != null && msg.updateTimeoutObject() == timeoutObj) {
            assert !msg.empty() : msg;

            clientCacheChanges.remove();

            msg.checkCachesExist(caches.registeredCaches.keySet());

            try {
                if (!msg.empty())
                    cctx.discovery().sendCustomEvent(msg);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send discovery event: " + e, e);
            }
        }
    }

    /**
     * @param startedCaches Started caches.
     * @param closedCaches Closed caches.
     */
    private void scheduleClientChangeMessage(Map<Integer, Boolean> startedCaches, Set<Integer> closedCaches) {
        ClientCacheChangeDiscoveryMessage msg = clientCacheChanges.get();

        if (msg == null) {
            msg = new ClientCacheChangeDiscoveryMessage(startedCaches, closedCaches);

            clientCacheChanges.set(msg);
        }
        else {
            msg.merge(startedCaches, closedCaches);

            if (msg.empty()) {
                cctx.time().removeTimeoutObject(msg.updateTimeoutObject());

                clientCacheChanges.remove();

                return;
            }
        }

        if (msg.updateTimeoutObject() != null)
            cctx.time().removeTimeoutObject(msg.updateTimeoutObject());

        long timeout = clientCacheMsgTimeout;

        if (timeout <= 0)
            timeout = 10_000;

        ClientCacheUpdateTimeout timeoutObj = new ClientCacheUpdateTimeout(cctx, timeout);

        msg.updateTimeoutObject(timeoutObj);

        cctx.time().addTimeoutObject(timeoutObj);
    }

    /**
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Exchange actions.
     */
    public void onCustomMessageNoAffinityChange(
        GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        @Nullable final ExchangeActions exchActions
    ) {
        final ExchangeDiscoveryEvents evts = fut.context().events();

        forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) {
                if (exchActions != null && exchActions.cacheGroupStopping(aff.groupId()))
                    return;

                aff.clientEventTopologyChange(evts.lastEvent(), evts.topologyVersion());
            }
        });
    }

    /**
     * @param cctx Stopped cache context.
     */
    public void stopCacheOnReconnect(GridCacheContext cctx) {
        caches.registeredCaches.remove(cctx.cacheId());
    }

    /**
     * @param grpCtx Stopped cache group context.
     */
    public void stopCacheGroupOnReconnect(CacheGroupContext grpCtx) {
        caches.registeredGrps.remove(grpCtx.groupId());
    }

    /**
     * Called during the rollback of the exchange partitions procedure
     * in order to stop the given cache even if it's not fully initialized (e.g. failed on cache init stage).
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Cache change requests.
     */
    public void forceCloseCaches(
        GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        final ExchangeActions exchActions
    ) {
        assert exchActions != null && !exchActions.empty() && exchActions.cacheStartRequests().isEmpty(): exchActions;

        caches.updateCachesInfo(exchActions);

        processCacheStopRequests(fut, crd, exchActions, true);

        cctx.cache().forceCloseCaches(exchActions);
    }

    /**
     * Called on exchange initiated for cache start/stop request.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Cache change requests.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheChangeRequest(
        GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        final ExchangeActions exchActions
    ) throws IgniteCheckedException {
        assert exchActions != null && !exchActions.empty() : exchActions;

        caches.updateCachesInfo(exchActions);

        // Affinity did not change for existing caches.
        onCustomMessageNoAffinityChange(fut, crd, exchActions);

        processCacheStartRequests(fut, crd, exchActions);

        Set<Integer> stoppedGrps = processCacheStopRequests(fut, crd, exchActions, false);

        if (stoppedGrps != null) {
            AffinityTopologyVersion notifyTopVer = null;

            synchronized (mux) {
                if (waitInfo != null) {
                    for (Integer grpId : stoppedGrps) {
                        boolean rmv = waitInfo.waitGrps.remove(grpId) != null;

                        if (rmv) {
                            notifyTopVer = waitInfo.topVer;

                            waitInfo.assignments.remove(grpId);
                        }
                    }
                }
            }

            if (notifyTopVer != null) {
                final AffinityTopologyVersion topVer = notifyTopVer;

                cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                    @Override public void run() {
                        onCacheGroupStopped(topVer);
                    }
                });
            }
        }

        ClientCacheChangeDiscoveryMessage msg = clientCacheChanges.get();

        if (msg != null) {
            msg.checkCachesExist(caches.registeredCaches.keySet());

            if (msg.empty())
                clientCacheChanges.remove();
        }
    }

    /**
     * Process cache start requests.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Cache change requests.
     * @throws IgniteCheckedException If failed.
     */
    private void processCacheStartRequests(
        GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        final ExchangeActions exchActions
    ) throws IgniteCheckedException {
        assert exchActions != null && !exchActions.empty() : exchActions;

        final ExchangeDiscoveryEvents evts = fut.context().events();

        for (ExchangeActions.CacheActionData action : exchActions.cacheStartRequests()) {
            DynamicCacheDescriptor cacheDesc = action.descriptor();

            DynamicCacheChangeRequest req = action.request();

            boolean startCache;

            NearCacheConfiguration nearCfg = null;

            if (req.locallyConfigured() || (cctx.localNodeId().equals(req.initiatingNodeId()) && !exchActions.activate())) {
                startCache = true;

                nearCfg = req.nearCacheConfiguration();
            }
            else {
                // Cache should not be started
                assert cctx.cacheContext(cacheDesc.cacheId()) == null
                    : "Starting cache has not null context: " + cacheDesc.cacheName();

                IgniteCacheProxyImpl cacheProxy = (IgniteCacheProxyImpl) cctx.cache().jcacheProxy(req.cacheName());

                // If it has proxy then try to start it
                if (cacheProxy != null) {
                    // Cache should be in restarting mode
                    assert cacheProxy.isRestarting()
                        : "Cache has non restarting proxy " + cacheProxy;

                    startCache = true;
                }
                else {
                    startCache = CU.affinityNode(cctx.localNode(),
                        cacheDesc.groupDescriptor().config().getNodeFilter());
                }
            }

            try {
                if (startCache) {
                    cctx.cache().prepareCacheStart(req.startCacheConfiguration(),
                        cacheDesc,
                        nearCfg,
                        evts.topologyVersion(),
                        req.disabledAfterStart());

                    if (fut.cacheAddedOnExchange(cacheDesc.cacheId(), cacheDesc.receivedFrom())) {
                        if (fut.events().discoveryCache().cacheGroupAffinityNodes(cacheDesc.groupId()).isEmpty())
                            U.quietAndWarn(log, "No server nodes found for cache client: " + req.cacheName());
                    }
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to initialize cache. Will try to rollback cache start routine. " +
                    "[cacheName=" + req.cacheName() + ']', e);

                cctx.cache().closeCaches(Collections.singleton(req.cacheName()), false);

                cctx.cache().completeCacheStartFuture(req, false, e);
            }
        }

        Set<Integer> gprs = new HashSet<>();

        for (ExchangeActions.CacheActionData action : exchActions.cacheStartRequests()) {
            int grpId = action.descriptor().groupId();

            if (gprs.add(grpId)) {
                if (crd)
                    initStartedGroupOnCoordinator(fut, action.descriptor().groupDescriptor());
                else {
                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    if (grp != null && !grp.isLocal() && grp.localStartVersion().equals(fut.initialVersion())) {
                        assert grp.affinity().lastVersion().equals(AffinityTopologyVersion.NONE) : grp.affinity().lastVersion();

                        initAffinity(caches.group(grp.groupId()), grp.affinity(), fut);
                    }
                }
            }
        }
    }

    /**
     * Process cache stop requests.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @param exchActions Cache change requests.
     * @param forceClose
     * @return Set of cache groups to be stopped.
     */
    private Set<Integer> processCacheStopRequests(
        GridDhtPartitionsExchangeFuture fut,
        boolean crd,
        final ExchangeActions exchActions,
        boolean forceClose
    ) {
        assert exchActions != null && !exchActions.empty() : exchActions;

        for (ExchangeActions.CacheActionData action : exchActions.cacheStopRequests())
            cctx.cache().blockGateway(action.request().cacheName(), true, action.request().restart());

        for (ExchangeActions.CacheGroupActionData action : exchActions.cacheGroupsToStop())
            cctx.exchange().clearClientTopology(action.descriptor().groupId());

        Set<Integer> stoppedGrps = null;

        if (crd) {
            for (ExchangeActions.CacheGroupActionData data : exchActions.cacheGroupsToStop()) {
                if (data.descriptor().config().getCacheMode() != LOCAL) {
                    CacheGroupHolder cacheGrp = grpHolders.remove(data.descriptor().groupId());

                    assert cacheGrp != null || forceClose : data.descriptor();

                    if (cacheGrp != null) {
                        if (stoppedGrps == null)
                            stoppedGrps = new HashSet<>();

                        stoppedGrps.add(cacheGrp.groupId());

                        cctx.io().removeHandler(true, cacheGrp.groupId(), GridDhtAffinityAssignmentResponse.class);
                    }
                }
            }
        }

        return stoppedGrps;
    }

    /**
     *
     */
    public void removeAllCacheInfo() {
        grpHolders.clear();

        caches.clear();
    }

    /**
     * Called when received {@link CacheAffinityChangeMessage} which should complete exchange.
     *
     * @param exchFut Exchange future.
     * @param crd Coordinator flag.
     * @param msg Affinity change message.
     */
    public void onExchangeChangeAffinityMessage(GridDhtPartitionsExchangeFuture exchFut,
        boolean crd,
        CacheAffinityChangeMessage msg) {
        if (log.isDebugEnabled()) {
            log.debug("Process exchange affinity change message [exchVer=" + exchFut.initialVersion() +
                ", msg=" + msg + ']');
        }

        assert exchFut.exchangeId().equals(msg.exchangeId()) : msg;

        final AffinityTopologyVersion topVer = exchFut.initialVersion();

        final Map<Integer, Map<Integer, List<UUID>>> assignment = msg.assignmentChange();

        assert assignment != null;

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                List<List<ClusterNode>> idealAssignment = aff.idealAssignment();

                assert idealAssignment != null;

                Map<Integer, List<UUID>> cacheAssignment = assignment.get(aff.groupId());

                List<List<ClusterNode>> newAssignment;

                if (cacheAssignment != null) {
                    newAssignment = new ArrayList<>(idealAssignment);

                    for (Map.Entry<Integer, List<UUID>> e : cacheAssignment.entrySet())
                        newAssignment.set(e.getKey(), toNodes(topVer, e.getValue()));
                }
                else
                    newAssignment = idealAssignment;

                aff.initialize(topVer, cachedAssignment(aff, newAssignment, affCache));
            }
        });
    }

    /**
     * Called on exchange initiated by {@link CacheAffinityChangeMessage} which sent after rebalance finished.
     *
     * @param exchFut Exchange future.
     * @param crd Coordinator flag.
     * @param msg Message.
     * @throws IgniteCheckedException If failed.
     */
    public void onChangeAffinityMessage(final GridDhtPartitionsExchangeFuture exchFut,
        boolean crd,
        final CacheAffinityChangeMessage msg)
        throws IgniteCheckedException {
        assert msg.topologyVersion() != null && msg.exchangeId() == null : msg;

        final AffinityTopologyVersion topVer = exchFut.initialVersion();

        if (log.isDebugEnabled()) {
            log.debug("Process affinity change message [exchVer=" + topVer +
                ", msgVer=" + msg.topologyVersion() + ']');
        }

        final Map<Integer, Map<Integer, List<UUID>>> affChange = msg.assignmentChange();

        assert !F.isEmpty(affChange) : msg;

        final Map<Integer, IgniteUuid> deploymentIds = msg.cacheDeploymentIds();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                AffinityTopologyVersion affTopVer = aff.lastVersion();

                assert affTopVer.topologyVersion() > 0 : affTopVer;

                CacheGroupDescriptor desc = caches.group(aff.groupId());

                assert desc != null : aff.cacheOrGroupName();

                IgniteUuid deploymentId = desc.deploymentId();

                if (!deploymentId.equals(deploymentIds.get(aff.groupId()))) {
                    aff.clientEventTopologyChange(exchFut.firstEvent(), topVer);

                    return;
                }

                Map<Integer, List<UUID>> change = affChange.get(aff.groupId());

                if (change != null) {
                    assert !change.isEmpty() : msg;

                    List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

                    List<List<ClusterNode>> assignment = new ArrayList<>(curAff);

                    for (Map.Entry<Integer, List<UUID>> e : change.entrySet()) {
                        Integer part = e.getKey();

                        List<ClusterNode> nodes = toNodes(topVer, e.getValue());

                        assert !nodes.equals(assignment.get(part)) : "Assignment did not change " +
                            "[cacheGrp=" + aff.cacheOrGroupName() +
                            ", part=" + part +
                            ", cur=" + F.nodeIds(assignment.get(part)) +
                            ", new=" + F.nodeIds(nodes) +
                            ", exchVer=" + exchFut.initialVersion() +
                            ", msgVer=" + msg.topologyVersion() +
                            ']';

                        assignment.set(part, nodes);
                    }

                    aff.initialize(topVer, cachedAssignment(aff, assignment, affCache));
                }
                else
                    aff.clientEventTopologyChange(exchFut.firstEvent(), topVer);
            }
        });
    }

    /**
     * Called on exchange initiated by client node join/fail.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onClientEvent(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        boolean locJoin = fut.firstEvent().eventNode().isLocal();

        if (!locJoin) {
            forAllCacheGroups(crd, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    AffinityTopologyVersion topVer = fut.initialVersion();

                    aff.clientEventTopologyChange(fut.firstEvent(), topVer);
                }
            });
        }
        else
            fetchAffinityOnJoin(fut);
    }

    /**
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(fut.id(), fut);

        assert old == null : "More than one thread is trying to fetch partition assignments [fut=" + fut +
            ", allFuts=" + pendingAssignmentFetchFuts + ']';
    }

    /**
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(GridDhtAssignmentFetchFuture fut) {
        boolean rmv = pendingAssignmentFetchFuts.remove(fut.id(), fut);

        assert rmv : "Failed to remove assignment fetch future: " + fut.id();
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + nodeId + ", res=" + res + ']');

        GridDhtAssignmentFetchFuture fut = pendingAssignmentFetchFuts.get(res.futureId());

        if (fut != null)
            fut.onResponse(nodeId, res);
    }

    /**
     * @param c Cache closure.
     * @throws IgniteCheckedException If failed
     */
    private void forAllRegisteredCacheGroups(IgniteInClosureX<CacheGroupDescriptor> c) throws IgniteCheckedException {
        for (CacheGroupDescriptor cacheDesc : caches.allGroups()) {
            if (cacheDesc.config().getCacheMode() == LOCAL)
                continue;

            c.applyx(cacheDesc);
        }
    }

    /**
     * @param crd Coordinator flag.
     * @param c Closure.
     */
    private void forAllCacheGroups(boolean crd, IgniteInClosureX<GridAffinityAssignmentCache> c) {
        if (crd) {
            for (CacheGroupHolder grp : grpHolders.values())
                c.apply(grp.affinity());
        }
        else {
            for (CacheGroupContext grp : cctx.kernalContext().cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                c.apply(grp.affinity());
            }
        }
    }

    /**
     * @param fut Exchange future.
     * @param grpDesc Cache group descriptor.
     * @throws IgniteCheckedException If failed.
     */
    private void initStartedGroupOnCoordinator(GridDhtPartitionsExchangeFuture fut, final CacheGroupDescriptor grpDesc)
        throws IgniteCheckedException {
        assert grpDesc != null && grpDesc.groupId() != 0 : grpDesc;

        if (grpDesc.config().getCacheMode() == LOCAL)
            return;

        int grpId = grpDesc.groupId();

        CacheGroupHolder grpHolder = grpHolders.get(grpId);

        CacheGroupContext grp = cctx.kernalContext().cache().cacheGroup(grpId);

        if (grpHolder == null) {
            grpHolder = grp != null ?
                new CacheGroupHolder1(grp, null) :
                CacheGroupHolder2.create(cctx, grpDesc, fut.initialVersion(), null);

            CacheGroupHolder old = grpHolders.put(grpId, grpHolder);

            assert old == null : old;

            calculateAndInit(fut.events(), grpHolder.affinity(), fut.initialVersion());
        }
        else if (grpHolder.client() && grp != null) {
            assert grpHolder.affinity().idealAssignment() != null;

            grpHolder = new CacheGroupHolder1(grp, grpHolder.affinity());

            grpHolders.put(grpId, grpHolder);
        }
    }

    /**
     * Initialized affinity for cache received from node joining on this exchange.
     *
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     * @param descs Cache descriptors.
     * @throws IgniteCheckedException If failed.
     */
    public void initStartedCaches(
        boolean crd,
        final GridDhtPartitionsExchangeFuture fut,
        Collection<DynamicCacheDescriptor> descs
    ) throws IgniteCheckedException {
        caches.initStartedCaches(descs);

        if (fut.context().mergeExchanges())
            return;

        if (crd) {
            forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                    CacheGroupHolder cache = groupHolder(fut.initialVersion(), desc);

                    if (cache.affinity().lastVersion().equals(AffinityTopologyVersion.NONE))
                        calculateAndInit(fut.events(), cache.affinity(), fut.initialVersion());
                }
            });
        }
        else {
            forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    if (aff.lastVersion().equals(AffinityTopologyVersion.NONE))
                        initAffinity(caches.group(aff.groupId()), aff, fut);
                }
            });
        }
    }

    /**
     * @param desc Cache group descriptor.
     * @param aff Affinity.
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void initAffinity(CacheGroupDescriptor desc,
        GridAffinityAssignmentCache aff,
        GridDhtPartitionsExchangeFuture fut)
        throws IgniteCheckedException {
        assert desc != null : aff.cacheOrGroupName();

        ExchangeDiscoveryEvents evts = fut.context().events();

        if (canCalculateAffinity(desc, aff, fut))
            calculateAndInit(evts, aff, evts.topologyVersion());
        else {
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                desc.groupId(),
                evts.topologyVersion(),
                evts.discoveryCache());

            fetchFut.init(false);

            fetchAffinity(evts.topologyVersion(), evts, evts.discoveryCache(), aff, fetchFut);
        }
    }

    /**
     * @param desc Cache group descriptor.
     * @param aff Affinity.
     * @param fut Exchange future.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(CacheGroupDescriptor desc,
        GridAffinityAssignmentCache aff,
        GridDhtPartitionsExchangeFuture fut) {
        assert desc != null : aff.cacheOrGroupName();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!aff.centralizedAffinityFunction())
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = fut.events().discoveryCache().cacheGroupAffinityNodes(aff.groupId());

        return fut.cacheGroupAddedOnExchange(aff.groupId(), desc.receivedFrom()) ||
            !fut.exchangeId().nodeId().equals(cctx.localNodeId()) ||
            (affNodes.isEmpty() || (affNodes.size() == 1 && affNodes.contains(cctx.localNode())));
    }

    /**
     * @param grpId Cache group ID.
     * @return Affinity assignments.
     */
    public GridAffinityAssignmentCache affinity(Integer grpId) {
        CacheGroupHolder grpHolder = grpHolders.get(grpId);

        assert grpHolder != null : debugGroupName(grpId);

        return grpHolder.affinity();
    }

    /**
     * Applies affinity diff from the received full message.
     *
     * @param fut Current exchange future.
     * @param msg Finish exchange message.
     */
    public void applyAffinityFromFullMessage(final GridDhtPartitionsExchangeFuture fut,
        final GridDhtPartitionsFullMessage msg) {
        final Map<Long, ClusterNode> nodesByOrder = new HashMap<>();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                ExchangeDiscoveryEvents evts = fut.context().events();

                Map<Integer, CacheGroupAffinityMessage> idealAffDiff = msg.idealAffinityDiff();

                List<List<ClusterNode>> idealAssignment = aff.calculate(evts.topologyVersion(), evts, evts.discoveryCache());

                CacheGroupAffinityMessage affMsg = idealAffDiff != null ? idealAffDiff.get(aff.groupId()) : null;

                List<List<ClusterNode>> newAssignment;

                if (affMsg != null) {
                    Map<Integer, GridLongList> diff = affMsg.assignmentsDiff();

                    assert !F.isEmpty(diff);

                    newAssignment = new ArrayList<>(idealAssignment);

                    for (Map.Entry<Integer, GridLongList> e : diff.entrySet()) {
                        GridLongList assign = e.getValue();

                        newAssignment.set(e.getKey(), CacheGroupAffinityMessage.toNodes(assign,
                            nodesByOrder,
                            evts.discoveryCache()));
                    }
                }
                else
                    newAssignment = idealAssignment;

                aff.initialize(evts.topologyVersion(), cachedAssignment(aff, newAssignment, affCache));
            }
        });
    }

    /**
     * @param fut Current exchange future.
     * @param msg Message finish message.
     * @param resTopVer Result topology version.
     */
    public void onLocalJoin(final GridDhtPartitionsExchangeFuture fut,
        GridDhtPartitionsFullMessage msg,
        final AffinityTopologyVersion resTopVer) {
        final Set<Integer> affReq = fut.context().groupsAffinityRequestOnJoin();

        final Map<Long, ClusterNode> nodesByOrder = new HashMap<>();

        final Map<Integer, CacheGroupAffinityMessage> receivedAff = msg.joinedNodeAffinity();

        assert F.isEmpty(affReq) || (!F.isEmpty(receivedAff) && receivedAff.size() >= affReq.size())
            : ("Requested and received affinity are different " +
                "[requestedCnt=" + (affReq != null ? affReq.size() : "none") +
                ", receivedCnt=" + (receivedAff != null ? receivedAff.size() : "none") +
                ", msg=" + msg + "]");

        forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
            @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                ExchangeDiscoveryEvents evts = fut.context().events();

                CacheGroupContext grp = cctx.cache().cacheGroup(aff.groupId());

                assert grp != null;

                if (affReq != null && affReq.contains(aff.groupId())) {
                    assert AffinityTopologyVersion.NONE.equals(aff.lastVersion());

                    CacheGroupAffinityMessage affMsg = receivedAff.get(aff.groupId());

                    assert affMsg != null;

                    List<List<ClusterNode>> assignments = affMsg.createAssignments(nodesByOrder, evts.discoveryCache());

                    assert resTopVer.equals(evts.topologyVersion());

                    List<List<ClusterNode>> idealAssign =
                        affMsg.createIdealAssignments(nodesByOrder, evts.discoveryCache());

                    if (idealAssign != null)
                        aff.idealAssignment(idealAssign);
                    else {
                        assert !aff.centralizedAffinityFunction();

                        // Calculate ideal assignments.
                        aff.calculate(evts.topologyVersion(), evts, evts.discoveryCache());
                    }

                    aff.initialize(evts.topologyVersion(), assignments);
                }
                else if (fut.cacheGroupAddedOnExchange(aff.groupId(), grp.receivedFrom()))
                    calculateAndInit(evts, aff, evts.topologyVersion());

                grp.topology().initPartitionsWhenAffinityReady(resTopVer, fut);
            }
        });
    }

    /**
     * @param fut Current exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoinWithExchangeMergeProtocol(GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        final ExchangeDiscoveryEvents evts = fut.context().events();

        assert fut.context().mergeExchanges();
        assert evts.hasServerJoin() && !evts.hasServerLeft();

        WaitRebalanceInfo waitRebalanceInfo = initAffinityOnNodeJoin(fut, crd);

        this.waitInfo = waitRebalanceInfo != null && !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

        WaitRebalanceInfo info = this.waitInfo;

        if (crd) {
            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node join [topVer=" + evts.topologyVersion() +
                    ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
            }
        }
    }

    /**
     * @param fut Current exchange future.
     * @return Computed difference with ideal affinity.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, CacheGroupAffinityMessage> onServerLeftWithExchangeMergeProtocol(
        final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException
    {
        final ExchangeDiscoveryEvents evts = fut.context().events();

        assert fut.context().mergeExchanges();
        assert evts.hasServerLeft();

        return onReassignmentEnforced(fut);
    }

    /**
     * Calculates affinity on coordinator for custom event types that require centralized assignment.
     *
     * @param fut Current exchange future.
     * @return Computed difference with ideal affinity.
     * @throws IgniteCheckedException If failed.
     */
    public Map<Integer, CacheGroupAffinityMessage> onCustomEventWithEnforcedAffinityReassignment(
        final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException
    {
        assert DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(fut.firstEvent());

        return onReassignmentEnforced(fut);
    }

    /**
     * Calculates new affinity assignment on coordinator and creates affinity diff messages for other nodes.
     *
     * @param fut Current exchange future.
     * @return Computed difference with ideal affinity.
     * @throws IgniteCheckedException If failed.
     */
    private Map<Integer, CacheGroupAffinityMessage> onReassignmentEnforced(
        final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException
    {
        final ExchangeDiscoveryEvents evts = fut.context().events();

        forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
            @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                AffinityTopologyVersion topVer = evts.topologyVersion();

                CacheGroupHolder cache = groupHolder(topVer, desc);

                List<List<ClusterNode>> assign = cache.affinity().calculate(topVer, evts, evts.discoveryCache());

                if (!cache.rebalanceEnabled || fut.cacheGroupAddedOnExchange(desc.groupId(), desc.receivedFrom()))
                    cache.affinity().initialize(topVer, assign);
            }
        });

        Map<Integer, Map<Integer, List<Long>>> diff = initAffinityBasedOnPartitionsAvailability(evts.topologyVersion(),
            fut,
            NODE_TO_ORDER,
            true);

        return CacheGroupAffinityMessage.createAffinityDiffMessages(diff);
    }

    /**
     * Called on exchange initiated by server node join.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onServerJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        assert !fut.firstEvent().eventNode().isClient();

        boolean locJoin = fut.firstEvent().eventNode().isLocal();

        WaitRebalanceInfo waitRebalanceInfo = null;

        if (locJoin) {
            if (crd) {
                forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                    @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                        AffinityTopologyVersion topVer = fut.initialVersion();

                        CacheGroupHolder grpHolder = groupHolder(topVer, desc);

                        calculateAndInit(fut.events(), grpHolder.affinity(), topVer);
                    }
                });
            }
            else
                fetchAffinityOnJoin(fut);
        }
        else
            waitRebalanceInfo = initAffinityOnNodeJoin(fut, crd);

        this.waitInfo = waitRebalanceInfo != null && !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

        WaitRebalanceInfo info = this.waitInfo;

        if (crd) {
            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node join [topVer=" + fut.initialVersion() +
                    ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
            }
        }
    }

    /**
     * @param fut Exchange future
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onBaselineTopologyChanged(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        assert !fut.firstEvent().eventNode().isClient();

        WaitRebalanceInfo waitRebalanceInfo = initAffinityOnNodeJoin(fut, crd);

        this.waitInfo = waitRebalanceInfo != null && !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

        WaitRebalanceInfo info = this.waitInfo;

        if (crd) {
            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node join [topVer=" + fut.initialVersion() +
                    ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
            }
        }
    }

    /**
     * @param grpIds Cache group IDs.
     * @return Cache names.
     */
    private String groupNames(Collection<Integer> grpIds) {
        StringBuilder names = new StringBuilder();

        for (Integer grpId : grpIds) {
            String name = caches.group(grpId).cacheOrGroupName();

            if (names.length() != 0)
                names.append(", ");

            names.append(name);
        }

        return names.toString();
    }

    /**
     * @param grpId Group ID.
     * @return Group name for debug purpose.
     */
    private String debugGroupName(int grpId) {
        CacheGroupDescriptor desc = caches.group(grpId);

        if (desc != null)
            return desc.cacheOrGroupName();
        else
            return "Unknown group: " + grpId;
    }

    /**
     * @param evts Discovery events.
     * @param aff Affinity.
     * @param topVer Topology version.
     */
    private void calculateAndInit(ExchangeDiscoveryEvents evts,
        GridAffinityAssignmentCache aff,
        AffinityTopologyVersion topVer)
    {
        List<List<ClusterNode>> assignment = aff.calculate(topVer, evts, evts.discoveryCache());

        aff.initialize(topVer, assignment);
    }

    /**
     * @param fut Exchange future.
     * @throws IgniteCheckedException If failed.
     */
    private void fetchAffinityOnJoin(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = fut.initialVersion();

        List<GridDhtAssignmentFetchFuture> fetchFuts = new ArrayList<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            if (fut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom())) {
                // In case if merge is allowed do not calculate affinity since it can change on exchange end.
                if (!fut.context().mergeExchanges())
                    calculateAndInit(fut.events(), grp.affinity(), topVer);
            }
            else {
                if (fut.context().fetchAffinityOnJoin()) {
                    CacheGroupDescriptor grpDesc = caches.group(grp.groupId());

                    assert grpDesc != null : grp.cacheOrGroupName();

                    GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                        grpDesc.groupId(),
                        topVer,
                        fut.events().discoveryCache());

                    fetchFut.init(false);

                    fetchFuts.add(fetchFut);
                }
                else {
                    if (fut.events().discoveryCache().serverNodes().size() > 0)
                        fut.context().addGroupAffinityRequestOnJoin(grp.groupId());
                    else
                        calculateAndInit(fut.events(), grp.affinity(), topVer);
                }
            }
        }

        for (int i = 0; i < fetchFuts.size(); i++) {
            GridDhtAssignmentFetchFuture fetchFut = fetchFuts.get(i);

            int grpId = fetchFut.groupId();

            fetchAffinity(topVer,
                fut.events(),
                fut.events().discoveryCache(),
                cctx.cache().cacheGroup(grpId).affinity(),
                fetchFut);
        }
    }

    /**
     * @param topVer Topology version.
     * @param events Discovery events.
     * @param discoCache Discovery data cache.
     * @param affCache Affinity.
     * @param fetchFut Affinity fetch future.
     * @throws IgniteCheckedException If failed.
     * @return Affinity assignment response.
     */
    private GridDhtAffinityAssignmentResponse fetchAffinity(AffinityTopologyVersion topVer,
        @Nullable ExchangeDiscoveryEvents events,
        DiscoCache discoCache,
        GridAffinityAssignmentCache affCache,
        GridDhtAssignmentFetchFuture fetchFut)
        throws IgniteCheckedException {
        assert affCache != null;

        GridDhtAffinityAssignmentResponse res = fetchFut.get();

        if (res == null) {
            List<List<ClusterNode>> aff = affCache.calculate(topVer, events, discoCache);

            affCache.initialize(topVer, aff);
        }
        else {
            List<List<ClusterNode>> idealAff = res.idealAffinityAssignment(discoCache);

            if (idealAff != null)
                affCache.idealAssignment(idealAff);
            else {
                assert !affCache.centralizedAffinityFunction();

                affCache.calculate(topVer, events, discoCache);
            }

            List<List<ClusterNode>> aff = res.affinityAssignment(discoCache);

            assert aff != null : res;

            affCache.initialize(topVer, aff);
        }

        return res;
    }

    /**
     * Called on exchange initiated by server node leave or custom event with centralized affinity assignment.
     *
     * @param fut Exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if affinity should be assigned by coordinator.
     */
    public boolean onCentralizedAffinityChange(final GridDhtPartitionsExchangeFuture fut, boolean crd) throws IgniteCheckedException {
        assert (fut.events().hasServerLeft() && !fut.firstEvent().eventNode().isClient()) ||
            DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(fut.firstEvent()) : fut.firstEvent();

        if (crd) {
            // Need initialize CacheGroupHolders if this node become coordinator on this exchange.
            forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                    CacheGroupHolder cache = groupHolder(fut.initialVersion(), desc);

                    cache.aff.calculate(fut.initialVersion(), fut.events(), fut.events().discoveryCache());
                }
            });
        }
        else {
            forAllCacheGroups(false, new IgniteInClosureX<GridAffinityAssignmentCache>() {
                @Override public void applyx(GridAffinityAssignmentCache aff) throws IgniteCheckedException {
                    aff.calculate(fut.initialVersion(), fut.events(), fut.events().discoveryCache());
                }
            });
        }

        synchronized (mux) {
            this.waitInfo = null;
        }

        return true;
    }

    /**
     * @param fut Exchange future.
     * @param newAff {@code True} if there are no older nodes with affinity info available.
     * @throws IgniteCheckedException If failed.
     * @return Future completed when caches initialization is done.
     */
    public IgniteInternalFuture<?> initCoordinatorCaches(
        final GridDhtPartitionsExchangeFuture fut,
        final boolean newAff
    ) throws IgniteCheckedException {
        final List<IgniteInternalFuture<AffinityTopologyVersion>> futs = new ArrayList<>();

        final AffinityTopologyVersion topVer = fut.initialVersion();

        forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
            @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                CacheGroupHolder grpHolder = grpHolders.get(desc.groupId());

                if (grpHolder != null)
                    return;

                // Need initialize holders and affinity if this node became coordinator during this exchange.
                int grpId = desc.groupId();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                if (grp == null) {
                    cctx.io().addCacheGroupHandler(desc.groupId(), GridDhtAffinityAssignmentResponse.class,
                        new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                            @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                                processAffinityAssignmentResponse(nodeId, res);
                            }
                        }
                    );

                    grpHolder = CacheGroupHolder2.create(cctx, desc, topVer, null);

                    final GridAffinityAssignmentCache aff = grpHolder.affinity();

                    if (newAff) {
                        if (!aff.lastVersion().equals(topVer))
                            calculateAndInit(fut.events(), aff, topVer);

                        grpHolder.topology(fut.context().events().discoveryCache()).beforeExchange(fut, true, false);
                    }
                    else {
                        List<GridDhtPartitionsExchangeFuture> exchFuts = cctx.exchange().exchangeFutures();

                        int idx = exchFuts.indexOf(fut);

                        assert idx >= 0 && idx < exchFuts.size() - 1 : "Invalid exchange futures state [cur=" + idx +
                            ", total=" + exchFuts.size() + ']';

                        final GridDhtPartitionsExchangeFuture prev = exchFuts.get(idx + 1);

                        assert prev.isDone() && prev.topologyVersion().compareTo(topVer) < 0 : prev;

                        if (log.isDebugEnabled()) {
                            log.debug("Need initialize affinity on coordinator [" +
                                "cacheGrp=" + desc.cacheOrGroupName() +
                                "prevAff=" + prev.topologyVersion() + ']');
                        }

                        GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cctx,
                            desc.groupId(),
                            prev.topologyVersion(),
                            prev.events().discoveryCache());

                        fetchFut.init(false);

                        final GridFutureAdapter<AffinityTopologyVersion> affFut = new GridFutureAdapter<>();

                        fetchFut.listen(new IgniteInClosureX<IgniteInternalFuture<GridDhtAffinityAssignmentResponse>>() {
                            @Override public void applyx(IgniteInternalFuture<GridDhtAffinityAssignmentResponse> fetchFut)
                                throws IgniteCheckedException {
                                fetchAffinity(prev.topologyVersion(),
                                    prev.events(),
                                    prev.events().discoveryCache(),
                                    aff,
                                    (GridDhtAssignmentFetchFuture)fetchFut);

                                aff.calculate(topVer, fut.events(), fut.events().discoveryCache());

                                affFut.onDone(topVer);
                            }
                        });

                        futs.add(affFut);
                    }
                }
                else {
                    grpHolder = new CacheGroupHolder1(grp, null);

                    if (newAff) {
                        GridAffinityAssignmentCache aff = grpHolder.affinity();

                        if (!aff.lastVersion().equals(topVer))
                            calculateAndInit(fut.events(), aff, topVer);

                        grpHolder.topology(fut.context().events().discoveryCache()).beforeExchange(fut, true, false);
                    }
                }

                CacheGroupHolder old = grpHolders.put(grpHolder.groupId(), grpHolder);

                assert old == null : old;
            }
        });

        if (!futs.isEmpty()) {
            GridCompoundFuture<AffinityTopologyVersion, ?> affFut = new GridCompoundFuture<>();

            for (IgniteInternalFuture<AffinityTopologyVersion> f : futs)
                affFut.add(f);

            affFut.markInitialized();

            return affFut;
        }

        return null;
    }

    /**
     * @param topVer Topology version.
     * @param desc Cache descriptor.
     * @return Cache holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheGroupHolder groupHolder(AffinityTopologyVersion topVer, final CacheGroupDescriptor desc)
        throws IgniteCheckedException {
        CacheGroupHolder cacheGrp = grpHolders.get(desc.groupId());

        if (cacheGrp != null)
            return cacheGrp;

        final CacheGroupContext grp = cctx.cache().cacheGroup(desc.groupId());

        if (grp == null) {
            cctx.io().addCacheGroupHandler(desc.groupId(), GridDhtAffinityAssignmentResponse.class,
                new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentResponse>() {
                    @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentResponse res) {
                        processAffinityAssignmentResponse(nodeId, res);
                    }
                }
            );

            cacheGrp = CacheGroupHolder2.create(cctx, desc, topVer, null);
        }
        else
            cacheGrp = new CacheGroupHolder1(grp, null);

        CacheGroupHolder old = grpHolders.put(desc.groupId(), cacheGrp);

        assert old == null : old;

        return cacheGrp;
    }

    /**
     * @param fut Current exchange future.
     * @param crd Coordinator flag.
     * @throws IgniteCheckedException If failed.
     * @return Rabalance info.
     */
    @Nullable private WaitRebalanceInfo initAffinityOnNodeJoin(final GridDhtPartitionsExchangeFuture fut, boolean crd)
        throws IgniteCheckedException {
        final ExchangeDiscoveryEvents evts = fut.context().events();

        final Map<Object, List<List<ClusterNode>>> affCache = new HashMap<>();

        if (!crd) {
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                boolean latePrimary = grp.rebalanceEnabled();

                initAffinityOnNodeJoin(evts,
                    evts.nodeJoined(grp.receivedFrom()),
                    grp.affinity(),
                    null,
                    latePrimary,
                    affCache);
            }

            return null;
        }
        else {
            final WaitRebalanceInfo waitRebalanceInfo = new WaitRebalanceInfo(evts.lastServerEventVersion());

            forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
                @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                    CacheGroupHolder cache = groupHolder(evts.topologyVersion(), desc);

                    boolean latePrimary = cache.rebalanceEnabled;

                    boolean grpAdded = evts.nodeJoined(desc.receivedFrom());

                    initAffinityOnNodeJoin(evts,
                        grpAdded,
                        cache.affinity(),
                        waitRebalanceInfo,
                        latePrimary,
                        affCache);

                    if (grpAdded) {
                        AffinityAssignment aff = cache.aff.cachedAffinity(cache.aff.lastVersion());

                        assert evts.topologyVersion().equals(aff.topologyVersion()) : "Unexpected version [" +
                            "grp=" + cache.aff.cacheOrGroupName() +
                            ", evts=" + evts.topologyVersion() +
                            ", aff=" + cache.aff.lastVersion() + ']';

                        Map<UUID, GridDhtPartitionMap> map = affinityFullMap(aff);

                        for (GridDhtPartitionMap map0 : map.values())
                            cache.topology(fut.context().events().discoveryCache()).update(fut.exchangeId(), map0, true);
                    }
                }
            });

            return waitRebalanceInfo;
        }
    }

    private Map<UUID, GridDhtPartitionMap> affinityFullMap(AffinityAssignment aff) {
        Map<UUID, GridDhtPartitionMap> map = new HashMap<>();

        for (int p = 0; p < aff.assignment().size(); p++) {
            HashSet<UUID> ids = aff.getIds(p);

            for (UUID nodeId : ids) {
                GridDhtPartitionMap partMap = map.get(nodeId);

                if (partMap == null) {
                    partMap = new GridDhtPartitionMap(nodeId,
                        1L,
                        aff.topologyVersion(),
                        new GridPartitionStateMap(),
                        false);

                    map.put(nodeId, partMap);
                }

                partMap.put(p, GridDhtPartitionState.OWNING);
            }
        }

        return map;
    }

    /**
     * @param evts Discovery events processed during exchange.
     * @param addedOnExchnage {@code True} if cache group was added during this exchange.
     * @param aff Affinity.
     * @param rebalanceInfo Rebalance information.
     * @param latePrimary If {@code true} delays primary assignment if it is not owner.
     * @param affCache Already calculated assignments (to reduce data stored in history).
     */
    private void initAffinityOnNodeJoin(
        ExchangeDiscoveryEvents evts,
        boolean addedOnExchnage,
        GridAffinityAssignmentCache aff,
        WaitRebalanceInfo rebalanceInfo,
        boolean latePrimary,
        Map<Object, List<List<ClusterNode>>> affCache
    ) {
        if (addedOnExchnage) {
            if (!aff.lastVersion().equals(evts.topologyVersion()))
                calculateAndInit(evts, aff, evts.topologyVersion());

            return;
        }

        AffinityTopologyVersion affTopVer = aff.lastVersion();

        assert affTopVer.topologyVersion() > 0 : "Affinity is not initialized [grp=" + aff.cacheOrGroupName() +
            ", topVer=" + affTopVer + ", node=" + cctx.localNodeId() + ']';

        List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

        assert aff.idealAssignment() != null : "Previous assignment is not available.";

        List<List<ClusterNode>> idealAssignment = aff.calculate(evts.topologyVersion(), evts, evts.discoveryCache());
        List<List<ClusterNode>> newAssignment = null;

        if (latePrimary) {
            for (int p = 0; p < idealAssignment.size(); p++) {
                List<ClusterNode> newNodes = idealAssignment.get(p);
                List<ClusterNode> curNodes = curAff.get(p);

                ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                    assert cctx.discovery().node(evts.topologyVersion(), curPrimary.id()) != null : curPrimary;

                    List<ClusterNode> nodes0 = latePrimaryAssignment(aff,
                        p,
                        curPrimary,
                        newNodes,
                        rebalanceInfo);

                    if (newAssignment == null)
                        newAssignment = new ArrayList<>(idealAssignment);

                    newAssignment.set(p, nodes0);
                }
            }
        }

        if (newAssignment == null)
            newAssignment = idealAssignment;

        aff.initialize(evts.topologyVersion(), cachedAssignment(aff, newAssignment, affCache));
    }

    /**
     * @param aff Assignment cache.
     * @param assign Assignment.
     * @param affCache Assignments already calculated for other caches.
     * @return Assignment.
     */
    private List<List<ClusterNode>> cachedAssignment(GridAffinityAssignmentCache aff,
        List<List<ClusterNode>> assign,
        Map<Object, List<List<ClusterNode>>> affCache) {
        List<List<ClusterNode>> assign0 = affCache.get(aff.similarAffinityKey());

        if (assign0 != null && assign0.equals(assign))
            assign = assign0;
        else
            affCache.put(aff.similarAffinityKey(), assign);

        return assign;
    }

    /**
     * @param aff Cache.
     * @param part Partition.
     * @param curPrimary Current primary.
     * @param newNodes New ideal assignment.
     * @param rebalance Rabalance information holder.
     * @return Assignment.
     */
    private List<ClusterNode> latePrimaryAssignment(
        GridAffinityAssignmentCache aff,
        int part,
        ClusterNode curPrimary,
        List<ClusterNode> newNodes,
        WaitRebalanceInfo rebalance) {
        assert curPrimary != null;
        assert !F.isEmpty(newNodes);
        assert !curPrimary.equals(newNodes.get(0));

        List<ClusterNode> nodes0 = new ArrayList<>(newNodes.size() + 1);

        nodes0.add(curPrimary);

        for (int i = 0; i < newNodes.size(); i++) {
            ClusterNode node = newNodes.get(i);

            if (!node.equals(curPrimary))
                nodes0.add(node);
        }

        if (rebalance != null)
            rebalance.add(aff.groupId(), part, newNodes.get(0).id(), newNodes);

        return nodes0;
    }

    /**
     * @param fut Exchange future.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    public IgniteInternalFuture<Map<Integer, Map<Integer, List<UUID>>>> initAffinityOnNodeLeft(
        final GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        assert !fut.context().mergeExchanges();

        IgniteInternalFuture<?> initFut = initCoordinatorCaches(fut, false);

        if (initFut != null && !initFut.isDone()) {
            final GridFutureAdapter<Map<Integer, Map<Integer, List<UUID>>>> resFut = new GridFutureAdapter<>();

            initFut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> initFut) {
                    try {
                        resFut.onDone(initAffinityBasedOnPartitionsAvailability(fut.initialVersion(), fut, NODE_TO_ID, false));
                    }
                    catch (IgniteCheckedException e) {
                        resFut.onDone(e);
                    }
                }
            });

            return resFut;
        }
        else
            return new GridFinishedFuture<>(initAffinityBasedOnPartitionsAvailability(fut.initialVersion(), fut, NODE_TO_ID, false));
    }

    /**
     * Initializes current affinity assignment based on partitions availability.
     * Nodes that have most recent data will be considered affinity nodes.
     *
     * @param topVer Topology version.
     * @param fut Exchange future.
     * @param c Closure converting affinity diff.
     * @param initAff {@code True} if need initialize affinity.
     * @return Affinity assignment.
     * @throws IgniteCheckedException If failed.
     */
    private <T> Map<Integer, Map<Integer, List<T>>> initAffinityBasedOnPartitionsAvailability(final AffinityTopologyVersion topVer,
        final GridDhtPartitionsExchangeFuture fut,
        final IgniteClosure<ClusterNode, T> c,
        final boolean initAff)
        throws IgniteCheckedException {
        final boolean enforcedCentralizedAssignment =
            DiscoveryCustomEvent.requiresCentralizedAffinityAssignment(fut.firstEvent());

        final WaitRebalanceInfo waitRebalanceInfo = enforcedCentralizedAssignment ?
            new WaitRebalanceInfo(fut.exchangeId().topologyVersion()) :
            new WaitRebalanceInfo(fut.context().events().lastServerEventVersion());

        final Collection<ClusterNode> aliveNodes = fut.context().events().discoveryCache().serverNodes();

        final Map<Integer, Map<Integer, List<T>>> assignment = new HashMap<>();

        forAllRegisteredCacheGroups(new IgniteInClosureX<CacheGroupDescriptor>() {
            @Override public void applyx(CacheGroupDescriptor desc) throws IgniteCheckedException {
                CacheGroupHolder grpHolder = groupHolder(topVer, desc);

                if (!grpHolder.rebalanceEnabled ||
                    (fut.cacheGroupAddedOnExchange(desc.groupId(), desc.receivedFrom()) && !enforcedCentralizedAssignment))
                    return;

                AffinityTopologyVersion affTopVer = grpHolder.affinity().lastVersion();

                assert (affTopVer.topologyVersion() > 0 && !affTopVer.equals(topVer)) || enforcedCentralizedAssignment :
                    "Invalid affinity version [last=" + affTopVer + ", futVer=" + topVer + ", grp=" + desc.cacheOrGroupName() + ']';

                List<List<ClusterNode>> curAssignment = grpHolder.affinity().assignments(affTopVer);
                List<List<ClusterNode>> newAssignment = grpHolder.affinity().idealAssignment();

                assert newAssignment != null;

                List<List<ClusterNode>> newAssignment0 = initAff ? new ArrayList<>(newAssignment) : null;

                GridDhtPartitionTopology top = grpHolder.topology(fut.context().events().discoveryCache());

                Map<Integer, List<T>> cacheAssignment = null;

                for (int p = 0; p < newAssignment.size(); p++) {
                    List<ClusterNode> newNodes = newAssignment.get(p);
                    List<ClusterNode> curNodes = curAssignment.get(p);

                    assert aliveNodes.containsAll(newNodes) : "Invalid new assignment [grp=" + grpHolder.aff.cacheOrGroupName() +
                        ", nodes=" + newNodes +
                        ", topVer=" + fut.context().events().discoveryCache().version() +
                        ", evts=" + fut.context().events().events() + "]";

                    ClusterNode curPrimary = curNodes.size() > 0 ? curNodes.get(0) : null;
                    ClusterNode newPrimary = newNodes.size() > 0 ? newNodes.get(0) : null;

                    List<ClusterNode> newNodes0 = null;

                    assert newPrimary == null || aliveNodes.contains(newPrimary) : "Invalid new primary [" +
                        "grp=" + desc.cacheOrGroupName() +
                        ", node=" + newPrimary +
                        ", topVer=" + topVer + ']';

                    List<ClusterNode> owners = top.owners(p);

                    // It is essential that curPrimary node has partition in OWNING state.
                    if (!owners.isEmpty() && !owners.contains(curPrimary))
                        curPrimary = owners.get(0);

                    if (curPrimary != null && newPrimary != null && !curPrimary.equals(newPrimary)) {
                        if (aliveNodes.contains(curPrimary)) {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
                                    p,
                                    curPrimary,
                                    newNodes,
                                    waitRebalanceInfo);
                            }
                        }
                        else {
                            GridDhtPartitionState state = top.partitionState(newPrimary.id(), p);

                            if (state != GridDhtPartitionState.OWNING) {
                                for (int i = 1; i < curNodes.size(); i++) {
                                    ClusterNode curNode = curNodes.get(i);

                                    if (top.partitionState(curNode.id(), p) == GridDhtPartitionState.OWNING &&
                                        aliveNodes.contains(curNode)) {
                                        newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
                                            p,
                                            curNode,
                                            newNodes,
                                            waitRebalanceInfo);

                                        break;
                                    }
                                }

                                if (newNodes0 == null) {
                                    for (ClusterNode owner : owners) {
                                        if (aliveNodes.contains(owner)) {
                                            newNodes0 = latePrimaryAssignment(grpHolder.affinity(),
                                                p,
                                                owner,
                                                newNodes,
                                                waitRebalanceInfo);

                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (newNodes0 != null) {
                        assert aliveNodes.containsAll(newNodes0) : "Invalid late assignment [grp=" + grpHolder.aff.cacheOrGroupName() +
                            ", nodes=" + newNodes +
                            ", topVer=" + fut.context().events().discoveryCache().version() +
                            ", evts=" + fut.context().events().events() + "]";

                        if (newAssignment0 != null)
                            newAssignment0.set(p, newNodes0);

                        if (cacheAssignment == null)
                            cacheAssignment = new HashMap<>();

                        List<T> n = new ArrayList<>(newNodes0.size());

                        for (int i = 0; i < newNodes0.size(); i++)
                            n.add(c.apply(newNodes0.get(i)));

                        cacheAssignment.put(p, n);
                    }
                }

                if (cacheAssignment != null)
                    assignment.put(grpHolder.groupId(), cacheAssignment);

                if (initAff)
                    grpHolder.affinity().initialize(topVer, newAssignment0);
            }
        });

        synchronized (mux) {
            this.waitInfo = !waitRebalanceInfo.empty() ? waitRebalanceInfo : null;

            WaitRebalanceInfo info = this.waitInfo;

            if (log.isDebugEnabled()) {
                log.debug("Computed new affinity after node left [topVer=" + topVer +
                    ", waitGrps=" + (info != null ? groupNames(info.waitGrps.keySet()) : null) + ']');
            }
        }

        return assignment;
    }

    /**
     * @return All registered cache groups.
     */
    public Map<Integer, CacheGroupDescriptor> cacheGroups() {
        return caches.registeredGrps;
    }

    /**
     * @return All registered cache groups.
     */
    public Map<Integer, DynamicCacheDescriptor> caches() {
        return caches.registeredCaches;
    }

    /**
     * @param grpId Cache group ID
     * @return Cache affinity cache.
     */
    @Nullable public GridAffinityAssignmentCache groupAffinity(int grpId) {
        CacheGroupHolder grpHolder = grpHolders.get(grpId);

        return grpHolder != null ? grpHolder.affinity() : null;
    }

    /**
     *
     */
    public void dumpDebugInfo() {
        if (!pendingAssignmentFetchFuts.isEmpty()) {
            U.warn(log, "Pending assignment fetch futures:");

            for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                U.warn(log, ">>> " + fut);
        }
    }

    /**
     * @param nodes Nodes.
     * @return IDs.
     */
    private static List<UUID> toIds0(List<ClusterNode> nodes) {
        List<UUID> partIds = new ArrayList<>(nodes.size());

        for (int i = 0; i < nodes.size(); i++)
            partIds.add(nodes.get(i).id());

        return partIds;
    }

    /**
     * @param topVer Topology version.
     * @param ids IDs.
     * @return Nodes.
     */
    private List<ClusterNode> toNodes(AffinityTopologyVersion topVer, List<UUID> ids) {
        List<ClusterNode> nodes = new ArrayList<>(ids.size());

        for (int i = 0; i < ids.size(); i++) {
            UUID id = ids.get(i);

            ClusterNode node = cctx.discovery().node(topVer, id);

            assert node != null : "Failed to get node [id=" + id +
                ", topVer=" + topVer +
                ", locNode=" + cctx.localNode() +
                ", allNodes=" + cctx.discovery().nodes(topVer) + ']';

            nodes.add(node);
        }

        return nodes;
    }

    /**
     *
     */
    abstract static class CacheGroupHolder {
        /** */
        private final GridAffinityAssignmentCache aff;

        /** */
        private final boolean rebalanceEnabled;

        /**
         * @param rebalanceEnabled Cache rebalance flag.
         * @param aff Affinity cache.
         * @param initAff Existing affinity cache.
         */
        CacheGroupHolder(boolean rebalanceEnabled,
            GridAffinityAssignmentCache aff,
            @Nullable GridAffinityAssignmentCache initAff) {
            this.aff = aff;

            if (initAff != null)
                aff.init(initAff);

            this.rebalanceEnabled = rebalanceEnabled;
        }

        /**
         * @return Client holder flag.
         */
        abstract boolean client();

        /**
         * @return Group ID.
         */
        int groupId() {
            return aff.groupId();
        }

        /**
         * @return Partitions number.
         */
        int partitions() {
            return aff.partitions();
        }

        /**
         * @param discoCache Discovery data cache.
         * @return Cache topology.
         */
        abstract GridDhtPartitionTopology topology(DiscoCache discoCache);

        /**
         * @return Affinity.
         */
        GridAffinityAssignmentCache affinity() {
            return aff;
        }
    }

    /**
     * Created cache is started on coordinator.
     */
    private class CacheGroupHolder1 extends CacheGroupHolder {
        /** */
        private final CacheGroupContext grp;

        /**
         * @param grp Cache group.
         * @param initAff Current affinity.
         */
        CacheGroupHolder1(CacheGroupContext grp, @Nullable GridAffinityAssignmentCache initAff) {
            super(grp.rebalanceEnabled(), grp.affinity(), initAff);

            assert !grp.isLocal() : grp;

            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(DiscoCache discoCache) {
            return grp.topology();
        }
    }

    /**
     * Created if cache is not started on coordinator.
     */
    private static class CacheGroupHolder2 extends CacheGroupHolder {
        /** */
        private final GridCacheSharedContext cctx;

        /**
         * @param cctx Context.
         * @param grpDesc Cache group descriptor.
         * @param topVer Current exchange version.
         * @param initAff Current affinity.
         * @return Cache holder.
         * @throws IgniteCheckedException If failed.
         */
        static CacheGroupHolder2 create(
            GridCacheSharedContext cctx,
            CacheGroupDescriptor grpDesc,
            AffinityTopologyVersion topVer,
            @Nullable GridAffinityAssignmentCache initAff) throws IgniteCheckedException {
            assert grpDesc != null;
            assert !cctx.kernalContext().clientNode();

            CacheConfiguration<?, ?> ccfg = grpDesc.config();

            assert ccfg != null : grpDesc;
            assert ccfg.getCacheMode() != LOCAL : ccfg.getName();

            assert !cctx.discovery().cacheGroupAffinityNodes(grpDesc.groupId(),
                topVer).contains(cctx.localNode()) : grpDesc.cacheOrGroupName();

            AffinityFunction affFunc = cctx.cache().clone(ccfg.getAffinity());

            cctx.kernalContext().resource().injectGeneric(affFunc);
            cctx.kernalContext().resource().injectCacheName(affFunc, ccfg.getName());

            U.startLifecycleAware(F.asList(affFunc));

            GridAffinityAssignmentCache aff = new GridAffinityAssignmentCache(cctx.kernalContext(),
                grpDesc.cacheOrGroupName(),
                grpDesc.groupId(),
                affFunc,
                ccfg.getNodeFilter(),
                ccfg.getBackups(),
                ccfg.getCacheMode() == LOCAL,
                grpDesc.persistenceEnabled());

            return new CacheGroupHolder2(ccfg.getRebalanceMode() != NONE, cctx, aff, initAff);
        }

        /**
         * @param rebalanceEnabled Rebalance flag.
         * @param cctx Context.
         * @param aff Affinity.
         * @param initAff Current affinity.
         */
        CacheGroupHolder2(
            boolean rebalanceEnabled,
            GridCacheSharedContext cctx,
            GridAffinityAssignmentCache aff,
            @Nullable GridAffinityAssignmentCache initAff) {
            super(rebalanceEnabled, aff, initAff);

            this.cctx = cctx;
        }

        /** {@inheritDoc} */
        @Override public boolean client() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public GridDhtPartitionTopology topology(DiscoCache discoCache) {
            return cctx.exchange().clientTopology(groupId(), discoCache);
        }
    }

    /**
     *
     */
    class WaitRebalanceInfo {
        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, Map<Integer, UUID>> waitGrps;

        /** */
        private Map<Integer, Map<Integer, List<ClusterNode>>> assignments;

        /** */
        private Map<Integer, IgniteUuid> deploymentIds;

        /**
         * @param topVer Topology version.
         */
        WaitRebalanceInfo(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /**
         * @return {@code True} if there are partitions waiting for rebalancing.
         */
        boolean empty() {
            if (waitGrps != null) {
                assert !waitGrps.isEmpty();
                assert waitGrps.size() == assignments.size();

                return false;
            }

            return true;
        }

        /**
         * @param grpId Group ID.
         * @param part Partition.
         * @param waitNode Node rebalancing data.
         * @param assignment New assignment.
         */
        void add(Integer grpId, Integer part, UUID waitNode, List<ClusterNode> assignment) {
            assert !F.isEmpty(assignment) : assignment;

            if (waitGrps == null) {
                waitGrps = new HashMap<>();
                assignments = new HashMap<>();
                deploymentIds = new HashMap<>();
            }

            Map<Integer, UUID> cacheWaitParts = waitGrps.get(grpId);

            if (cacheWaitParts == null) {
                waitGrps.put(grpId, cacheWaitParts = new HashMap<>());

                deploymentIds.put(grpId, caches.group(grpId).deploymentId());
            }

            cacheWaitParts.put(part, waitNode);

            Map<Integer, List<ClusterNode>> cacheAssignment = assignments.get(grpId);

            if (cacheAssignment == null)
                assignments.put(grpId, cacheAssignment = new HashMap<>());

            cacheAssignment.put(part, assignment);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "WaitRebalanceInfo [topVer=" + topVer +
                ", grps=" + (waitGrps != null ? waitGrps.keySet() : null) + ']';
        }
    }

    /**
     *
     */
    class CachesInfo {
        /** Registered cache groups (updated from exchange thread). */
        private final ConcurrentHashMap<Integer, CacheGroupDescriptor> registeredGrps = new ConcurrentHashMap<>();

        /** Registered caches (updated from exchange thread). */
        private final ConcurrentHashMap<Integer, DynamicCacheDescriptor> registeredCaches = new ConcurrentHashMap<>();

        /**
         * @param grps Registered groups.
         * @param caches Registered caches.
         */
        void init(Map<Integer, CacheGroupDescriptor> grps, Map<String, DynamicCacheDescriptor> caches) {
            for (CacheGroupDescriptor grpDesc : grps.values())
                registerGroup(grpDesc);

            for (DynamicCacheDescriptor cacheDesc : caches.values())
                registerCache(cacheDesc);
        }


        /**
         * @param desc Description.
         */
        private DynamicCacheDescriptor registerCache(DynamicCacheDescriptor desc) {
            saveCacheConfiguration(desc.cacheConfiguration(), desc.sql());

            return registeredCaches.put(desc.cacheId(), desc);
        }

        /**
         * @param grpDesc Group description.
         */
        private CacheGroupDescriptor registerGroup(CacheGroupDescriptor grpDesc) {
            return registeredGrps.put(grpDesc.groupId(), grpDesc);
        }

        /**
         * @return All registered groups.
         */
        Collection<CacheGroupDescriptor> allGroups() {
            return registeredGrps.values();
        }

        /**
         * @param grpId Group ID.
         * @return Group descriptor.
         */
        CacheGroupDescriptor group(int grpId) {
            CacheGroupDescriptor desc = registeredGrps.get(grpId);

            assert desc != null : grpId;

            return desc;
        }

        /**
          * @param descs Cache descriptor.
         */
        void initStartedCaches(Collection<DynamicCacheDescriptor> descs) {
            for (DynamicCacheDescriptor desc : descs) {
                CacheGroupDescriptor grpDesc = desc.groupDescriptor();

                if (!registeredGrps.containsKey(grpDesc.groupId()))
                    registerGroup(grpDesc);

                if (!registeredCaches.containsKey(desc.cacheId()))
                    registerCache(desc);
            }
        }

        /**
         * @param exchActions Exchange actions.
         */
        void updateCachesInfo(ExchangeActions exchActions) {
            for (ExchangeActions.CacheGroupActionData stopAction : exchActions.cacheGroupsToStop()) {
                CacheGroupDescriptor rmvd = registeredGrps.remove(stopAction.descriptor().groupId());

                assert rmvd != null : stopAction.descriptor().cacheOrGroupName();
            }

            for (ExchangeActions.CacheGroupActionData startAction : exchActions.cacheGroupsToStart()) {
                CacheGroupDescriptor old = registerGroup(startAction.descriptor());

                assert old == null : old;
            }

            for (ExchangeActions.CacheActionData req : exchActions.cacheStopRequests())
                registeredCaches.remove(req.descriptor().cacheId());

            for (ExchangeActions.CacheActionData req : exchActions.cacheStartRequests())
                registerCache(req.descriptor());
        }

        /**
         * @param cacheId Cache ID.
         * @return Cache descriptor if cache found.
         */
        @Nullable DynamicCacheDescriptor cache(Integer cacheId) {
            return registeredCaches.get(cacheId);
        }

        /**
         *
         */
        void clear() {
            registeredGrps.clear();

            registeredCaches.clear();
        }
    }

    /**
     * @param cfg cache configuration
     * @param sql SQL flag.
     */
    private void saveCacheConfiguration(CacheConfiguration<?, ?> cfg, boolean sql) {
        if (cctx.pageStore() != null && CU.isPersistentCache(cfg, cctx.gridConfig().getDataStorageConfiguration()) &&
            !cctx.kernalContext().clientNode()) {
            try {
                StoredCacheData data = new StoredCacheData(cfg);

                data.sql(sql);

                cctx.pageStore().storeCacheData(data, false);
            }
            catch (IgniteCheckedException e) {
                U.error(log(), "Error while saving cache configuration on disk, cfg = " + cfg, e);
            }
        }
    }
}
