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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityCentralizedFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryTopologySnapshot;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeRequest;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Future for exchanging partition maps.
 */
public class GridDhtPartitionsExchangeFuture extends GridFutureAdapter<AffinityTopologyVersion>
    implements Comparable<GridDhtPartitionsExchangeFuture>, GridDhtTopologyFuture {
    /** */
    public static final int DUMP_PENDING_OBJECTS_THRESHOLD =
        IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_DUMP_PENDING_OBJECTS_THRESHOLD, 10);

    /** */
    private static final long serialVersionUID = 0L;

    /** Dummy flag. */
    private final boolean dummy;

    /** Force preload flag. */
    private final boolean forcePreload;

    /** Dummy reassign flag. */
    private final boolean reassign;

    /** Discovery event. */
    private volatile DiscoveryEvent discoEvt;

    /** */
    @GridToStringInclude
    private final Collection<UUID> rcvdIds = new GridConcurrentHashSet<>();

    /** Remote nodes. */
    private volatile Collection<ClusterNode> rmtNodes;

    /** Remote nodes. */
    @GridToStringInclude
    private volatile Collection<UUID> rmtIds;

    /** Oldest node. */
    @GridToStringExclude
    private final AtomicReference<ClusterNode> oldestNode = new AtomicReference<>();

    /** ExchangeFuture id. */
    private final GridDhtPartitionExchangeId exchId;

    /** Init flag. */
    @GridToStringInclude
    private final AtomicBoolean init = new AtomicBoolean(false);

    /** Ready for reply flag. */
    @GridToStringInclude
    private final AtomicBoolean ready = new AtomicBoolean(false);

    /** Replied flag. */
    @GridToStringInclude
    private final AtomicBoolean replied = new AtomicBoolean(false);

    /** Timeout object. */
    @GridToStringExclude
    private volatile GridTimeoutObject timeoutObj;

    /** Cache context. */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private ReadWriteLock busyLock;

    /** */
    private AtomicBoolean added = new AtomicBoolean(false);

    /** Event latch. */
    @GridToStringExclude
    private CountDownLatch evtLatch = new CountDownLatch(1);

    /** */
    private GridFutureAdapter<Boolean> initFut;

    /** Topology snapshot. */
    private AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot = new AtomicReference<>();

    /** Last committed cache version before next topology version use. */
    private AtomicReference<GridCacheVersion> lastVer = new AtomicReference<>();

    /**
     * Messages received on non-coordinator are stored in case if this node
     * becomes coordinator.
     */
    private final Map<UUID, GridDhtPartitionsSingleMessage> singleMsgs = new ConcurrentHashMap8<>();

    /** Messages received from new coordinator. */
    private final Map<UUID, GridDhtPartitionsFullMessage> fullMsgs = new ConcurrentHashMap8<>();

    /** */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    @GridToStringInclude
    private volatile IgniteInternalFuture<?> partReleaseFut;

    /** */
    private final Object mux = new Object();

    /** Logger. */
    private IgniteLogger log;

    /** Dynamic cache change requests. */
    private Collection<DynamicCacheChangeRequest> reqs;

    /** Cache validation results. */
    private volatile Map<Integer, Boolean> cacheValidRes;

    /** Skip preload flag. */
    private boolean skipPreload;

    /** */
    private boolean clientOnlyExchange;

    /** Init timestamp. Used to track the amount of time spent to complete the future. */
    private long initTs;

    /**
     * Dummy future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param reassign Dummy reassign flag.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     */
    public GridDhtPartitionsExchangeFuture(
        GridCacheSharedContext cctx,
        boolean reassign,
        DiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId
    ) {
        dummy = true;
        forcePreload = false;

        this.exchId = exchId;
        this.reassign = reassign;
        this.discoEvt = discoEvt;
        this.cctx = cctx;

        onDone(exchId.topologyVersion());
    }

    /**
     * Force preload future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheSharedContext cctx, DiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId) {
        dummy = false;
        forcePreload = true;

        this.exchId = exchId;
        this.discoEvt = discoEvt;
        this.cctx = cctx;

        reassign = true;

        onDone(exchId.topologyVersion());
    }

    /**
     * @param cctx Cache context.
     * @param busyLock Busy lock.
     * @param exchId Exchange ID.
     * @param reqs Cache change requests.
     */
    public GridDhtPartitionsExchangeFuture(
        GridCacheSharedContext cctx,
        ReadWriteLock busyLock,
        GridDhtPartitionExchangeId exchId,
        Collection<DynamicCacheChangeRequest> reqs
    ) {
        assert busyLock != null;
        assert exchId != null;

        dummy = false;
        forcePreload = false;
        reassign = false;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;
        this.reqs = reqs;

        log = cctx.logger(getClass());

        initFut = new GridFutureAdapter<>();

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() + ", fut=" + this + ']');
    }

    /**
     * @param reqs Cache change requests.
     */
    public void cacheChangeRequests(Collection<DynamicCacheChangeRequest> reqs) {
        this.reqs = reqs;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        return exchId.topologyVersion();
    }

    /**
     * @return Skip preload flag.
     */
    public boolean skipPreload() {
        return skipPreload;
    }

    /**
     * @return Dummy flag.
     */
    public boolean dummy() {
        return dummy;
    }

    /**
     * @return Force preload flag.
     */
    public boolean forcePreload() {
        return forcePreload;
    }

    /**
     * @return Dummy reassign flag.
     */
    public boolean reassign() {
        return reassign;
    }

    /**
     * @return {@code True} if dummy reassign.
     */
    public boolean dummyReassign() {
        return (dummy() || forcePreload()) && reassign();
    }

    /**
     * @param cacheId Cache ID to check.
     * @param topVer Topology version.
     * @return {@code True} if cache was added during this exchange.
     */
    public boolean isCacheAdded(int cacheId, AffinityTopologyVersion topVer) {
        if (cacheStarted(cacheId))
            return true;

        GridCacheContext<?, ?> cacheCtx = cctx.cacheContext(cacheId);

        return cacheCtx != null && F.eq(cacheCtx.startTopologyVersion(), topVer);
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if non-client cache was added during this exchange.
     */
    private boolean cacheStarted(int cacheId) {
        if (!F.isEmpty(reqs)) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (req.start() && !req.clientStartOnly()) {
                    if (CU.cacheId(req.cacheName()) == cacheId)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if local client has been added.
     */
    public boolean isLocalClientAdded(int cacheId) {
        if (!F.isEmpty(reqs)) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (req.start() && F.eq(req.initiatingNodeId(), cctx.localNodeId())) {
                    if (CU.cacheId(req.cacheName()) == cacheId)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * @param cacheCtx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    private void initTopology(GridCacheContext cacheCtx) throws IgniteCheckedException {
        if (stopping(cacheCtx.cacheId()))
            return;

        if (canCalculateAffinity(cacheCtx)) {
            if (log.isDebugEnabled())
                log.debug("Will recalculate affinity [locNodeId=" + cctx.localNodeId() + ", exchId=" + exchId + ']');

            cacheCtx.affinity().calculateAffinity(exchId.topologyVersion(), discoEvt);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Will request affinity from remote node [locNodeId=" + cctx.localNodeId() + ", exchId=" +
                    exchId + ']');

            // Fetch affinity assignment from remote node.
            GridDhtAssignmentFetchFuture fetchFut = new GridDhtAssignmentFetchFuture(cacheCtx,
                exchId.topologyVersion(),
                CU.affinityNodes(cacheCtx, exchId.topologyVersion()));

            fetchFut.init();

            List<List<ClusterNode>> affAssignment = fetchFut.get();

            if (log.isDebugEnabled())
                log.debug("Fetched affinity from remote node, initializing affinity assignment [locNodeId=" +
                    cctx.localNodeId() + ", topVer=" + exchId.topologyVersion() + ']');

            if (affAssignment == null) {
                affAssignment = new ArrayList<>(cacheCtx.affinity().partitions());

                List<ClusterNode> empty = Collections.emptyList();

                for (int i = 0; i < cacheCtx.affinity().partitions(); i++)
                    affAssignment.add(empty);
            }

            cacheCtx.affinity().initializeAffinity(exchId.topologyVersion(), affAssignment);
        }
    }

    /**
     * @param cacheCtx Cache context.
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(GridCacheContext cacheCtx) {
        AffinityFunction affFunc = cacheCtx.config().getAffinity();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!U.hasAnnotation(affFunc, AffinityCentralizedFunction.class))
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = CU.affinityNodes(cacheCtx, exchId.topologyVersion());

        return cacheStarted(cacheCtx.cacheId()) ||
            !exchId.nodeId().equals(cctx.localNodeId()) ||
            (affNodes.size() == 1 && affNodes.contains(cctx.localNode()));
    }

    /**
     * @return {@code True}
     */
    public boolean onAdded() {
        return added.compareAndSet(false, true);
    }

    /**
     * Event callback.
     *
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     */
    public void onEvent(GridDhtPartitionExchangeId exchId, DiscoveryEvent discoEvt) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;

        evtLatch.countDown();
    }

    /**
     * @return Discovery event.
     */
    public DiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state (exchanger is stopping): " + this);

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * Starts activity.
     *
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    public void init() throws IgniteInterruptedCheckedException {
        if (isDone())
            return;

        if (init.compareAndSet(false, true)) {
            if (isDone())
                return;

            initTs = U.currentTimeMillis();

            try {
                // Wait for event to occur to make sure that discovery
                // will return corresponding nodes.
                U.await(evtLatch);

                assert discoEvt != null : this;
                assert !dummy && !forcePreload : this;

                ClusterNode oldest = CU.oldestAliveCacheServerNode(cctx, exchId.topologyVersion());

                oldestNode.set(oldest);

                if (!F.isEmpty(reqs))
                    blockGateways();

                startCaches();

                // True if client node joined or failed.
                boolean clientNodeEvt;

                if (F.isEmpty(reqs)) {
                    int type = discoEvt.type();

                    assert type == EVT_NODE_JOINED || type == EVT_NODE_LEFT || type == EVT_NODE_FAILED : discoEvt;

                    clientNodeEvt = CU.clientNode(discoEvt.eventNode());
                }
                else {
                    assert discoEvt.type() == EVT_DISCOVERY_CUSTOM_EVT : discoEvt;

                    boolean clientOnlyCacheEvt = true;

                    for (DynamicCacheChangeRequest req : reqs) {
                        if (req.clientStartOnly() || req.close())
                            continue;

                        clientOnlyCacheEvt = false;

                        break;
                    }

                    clientNodeEvt = clientOnlyCacheEvt;
                }

                if (clientNodeEvt) {
                    ClusterNode node = discoEvt.eventNode();

                    // Client need to initialize affinity for local join event or for stated client caches.
                    if (!node.isLocal() || clientCacheClose()) {
                        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                            if (cacheCtx.isLocal())
                                continue;

                            GridDhtPartitionTopology top = cacheCtx.topology();

                            top.updateTopologyVersion(exchId, this, -1, stopping(cacheCtx.cacheId()));

                            if (cacheCtx.affinity().affinityTopologyVersion() == AffinityTopologyVersion.NONE) {
                                initTopology(cacheCtx);

                                top.beforeExchange(this);
                            }
                            else
                                cacheCtx.affinity().clientEventTopologyChange(discoEvt, exchId.topologyVersion());

                            if (!exchId.isJoined())
                                cacheCtx.preloader().unwindUndeploys();
                        }

                        if (exchId.isLeft())
                            cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());

                        rmtIds = Collections.emptyList();
                        rmtNodes = Collections.emptyList();

                        onDone(exchId.topologyVersion());

                        skipPreload = cctx.kernalContext().clientNode();

                        return;
                    }
                }

                clientOnlyExchange = clientNodeEvt || cctx.kernalContext().clientNode();

                if (clientOnlyExchange) {
                    skipPreload = cctx.kernalContext().clientNode();

                    for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                        if (cacheCtx.isLocal())
                            continue;

                        GridDhtPartitionTopology top = cacheCtx.topology();

                        top.updateTopologyVersion(exchId, this, -1, stopping(cacheCtx.cacheId()));
                    }

                    for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                        if (cacheCtx.isLocal())
                            continue;

                        initTopology(cacheCtx);
                    }

                    if (oldest != null) {
                        rmtNodes = new ConcurrentLinkedQueue<>(CU.aliveRemoteServerNodesWithCaches(cctx,
                            exchId.topologyVersion()));

                        rmtIds = Collections.unmodifiableSet(new HashSet<>(F.nodeIds(rmtNodes)));

                        initFut.onDone(true);

                        if (log.isDebugEnabled())
                            log.debug("Initialized future: " + this);

                        if (cctx.localNode().equals(oldest)) {
                            for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                                boolean updateTop = !cacheCtx.isLocal() &&
                                    exchId.topologyVersion().equals(cacheCtx.startTopologyVersion());

                                if (updateTop) {
                                    for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
                                        if (top.cacheId() == cacheCtx.cacheId()) {
                                            cacheCtx.topology().update(exchId,
                                                top.partitionMap(true),
                                                top.updateCounters());

                                            break;
                                        }
                                    }

                                }
                            }

                            onDone(exchId.topologyVersion());
                        }
                        else
                            sendPartitions(oldest);
                    }
                    else {
                        rmtIds = Collections.emptyList();
                        rmtNodes = Collections.emptyList();

                        onDone(exchId.topologyVersion());
                    }

                    return;
                }

                assert oldestNode.get() != null;

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (isCacheAdded(cacheCtx.cacheId(), exchId.topologyVersion())) {
                        if (cacheCtx.discovery().cacheAffinityNodes(cacheCtx.name(), topologyVersion()).isEmpty())
                            U.quietAndWarn(log, "No server nodes found for cache client: " + cacheCtx.namex());
                    }

                    cacheCtx.preloader().onExchangeFutureAdded();
                }

                List<String> cachesWithoutNodes = null;

                if (exchId.isLeft()) {
                    for (String name : cctx.cache().cacheNames()) {
                        if (cctx.discovery().cacheAffinityNodes(name, topologyVersion()).isEmpty()) {
                            if (cachesWithoutNodes == null)
                                cachesWithoutNodes = new ArrayList<>();

                            cachesWithoutNodes.add(name);

                            // Fire event even if there is no client cache started.
                            if (cctx.gridEvents().isRecordable(EventType.EVT_CACHE_NODES_LEFT)) {
                                Event evt = new CacheEvent(
                                    name,
                                    cctx.localNode(),
                                    cctx.localNode(),
                                    "All server nodes have left the cluster.",
                                    EventType.EVT_CACHE_NODES_LEFT,
                                    0,
                                    false,
                                    null,
                                    null,
                                    null,
                                    null,
                                    false,
                                    null,
                                    false,
                                    null,
                                    null,
                                    null
                                );

                                cctx.gridEvents().record(evt);
                            }
                        }
                    }
                }

                if (cachesWithoutNodes != null) {
                    StringBuilder sb =
                        new StringBuilder("All server nodes for the following caches have left the cluster: ");

                    for (int i = 0; i < cachesWithoutNodes.size(); i++) {
                        String cache = cachesWithoutNodes.get(i);

                        sb.append('\'').append(cache).append('\'');

                        if (i != cachesWithoutNodes.size() - 1)
                            sb.append(", ");
                    }

                    U.quietAndWarn(log, sb.toString());

                    U.quietAndWarn(log, "Must have server nodes for caches to operate.");
                }

                assert discoEvt != null;

                assert exchId.nodeId().equals(discoEvt.eventNode().id());

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    GridClientPartitionTopology clientTop = cctx.exchange().clearClientTopology(
                        cacheCtx.cacheId());

                    long updSeq = clientTop == null ? -1 : clientTop.lastUpdateSequence();

                    // Update before waiting for locks.
                    if (!cacheCtx.isLocal())
                        cacheCtx.topology().updateTopologyVersion(exchId, this, updSeq, stopping(cacheCtx.cacheId()));
                }

                // Grab all alive remote nodes with order of equal or less than last joined node.
                rmtNodes = new ConcurrentLinkedQueue<>(CU.aliveRemoteServerNodesWithCaches(cctx,
                    exchId.topologyVersion()));

                rmtIds = Collections.unmodifiableSet(new HashSet<>(F.nodeIds(rmtNodes)));

                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> m : singleMsgs.entrySet())
                    // If received any messages, process them.
                    onReceive(m.getKey(), m.getValue());

                for (Map.Entry<UUID, GridDhtPartitionsFullMessage> m : fullMsgs.entrySet())
                    // If received any messages, process them.
                    onReceive(m.getKey(), m.getValue());

                AffinityTopologyVersion topVer = exchId.topologyVersion();

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    // Must initialize topology after we get discovery event.
                    initTopology(cacheCtx);

                    cacheCtx.preloader().onTopologyChanged(exchId.topologyVersion());

                    cacheCtx.preloader().updateLastExchangeFuture(this);
                }

                IgniteInternalFuture<?> partReleaseFut = cctx.partitionReleaseFuture(topVer);

                // Assign to class variable so it will be included into toString() method.
                this.partReleaseFut = partReleaseFut;

                if (exchId.isLeft())
                    cctx.mvcc().removeExplicitNodeLocks(exchId.nodeId(), exchId.topologyVersion());

                if (log.isDebugEnabled())
                    log.debug("Before waiting for partition release future: " + this);

                int dumpedObjects = 0;

                while (true) {
                    try {
                        partReleaseFut.get(2 * cctx.gridConfig().getNetworkTimeout(), TimeUnit.MILLISECONDS);

                        break;
                    }
                    catch (IgniteFutureTimeoutCheckedException ignored) {
                        // Print pending transactions and locks that might have led to hang.
                        if (dumpedObjects < DUMP_PENDING_OBJECTS_THRESHOLD) {
                            U.warn(log, "Failed to wait for partition release future [topVer=" + topologyVersion() +
                                ", node=" + cctx.localNodeId() + "]. Dumping pending objects that might be the cause: ");

                            try {
                                cctx.exchange().dumpDebugInfo();
                            }
                            catch (Exception e) {
                                U.error(log, "Failed to dump debug information: " + e, e);
                            }

                            if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                                U.dumpThreads(log);

                            dumpedObjects++;
                        }
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("After waiting for partition release future: " + this);

                IgniteInternalFuture<?> locksFut = cctx.mvcc().finishLocks(exchId.topologyVersion());

                dumpedObjects = 0;

                while (true) {
                    try {
                        locksFut.get(2 * cctx.gridConfig().getNetworkTimeout(), TimeUnit.MILLISECONDS);

                        break;
                    }
                    catch (IgniteFutureTimeoutCheckedException ignored) {
                        if (dumpedObjects < DUMP_PENDING_OBJECTS_THRESHOLD) {
                            U.warn(log, "Failed to wait for locks release future. " +
                                "Dumping pending objects that might be the cause: " + cctx.localNodeId());

                            U.warn(log, "Locked keys:");

                            for (IgniteTxKey key : cctx.mvcc().lockedKeys())
                                U.warn(log, "Locked key: " + key);

                            for (IgniteTxKey key : cctx.mvcc().nearLockedKeys())
                                U.warn(log, "Locked near key: " + key);

                            Map<IgniteTxKey, Collection<GridCacheMvccCandidate>> locks =
                                cctx.mvcc().unfinishedLocks(exchId.topologyVersion());

                            for (Map.Entry<IgniteTxKey, Collection<GridCacheMvccCandidate>> e : locks.entrySet())
                                U.warn(log, "Awaited locked entry [key=" + e.getKey() + ", mvcc=" + e.getValue() + ']');

                            dumpedObjects++;
                        }
                    }
                }

                boolean topChanged = discoEvt.type() != EVT_DISCOVERY_CUSTOM_EVT;

                for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    // Notify replication manager.
                    GridCacheContext drCacheCtx = cacheCtx.isNear() ? cacheCtx.near().dht().context() : cacheCtx;

                    if (drCacheCtx.isDrEnabled())
                        drCacheCtx.dr().beforeExchange(topVer, exchId.isLeft());

                    if (topChanged)
                        cacheCtx.continuousQueries().beforeExchange(exchId.topologyVersion());

                    // Partition release future is done so we can flush the write-behind store.
                    cacheCtx.store().forceFlush();

                    if (!exchId.isJoined())
                        // Process queued undeploys prior to sending/spreading map.
                        cacheCtx.preloader().unwindUndeploys();

                    GridDhtPartitionTopology top = cacheCtx.topology();

                    assert topVer.equals(top.topologyVersion()) :
                        "Topology version is updated only in this class instances inside single ExchangeWorker thread.";

                    top.beforeExchange(this);
                }

                for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
                    top.updateTopologyVersion(exchId, this, -1, stopping(top.cacheId()));

                    top.beforeExchange(this);
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                onDone(e);

                throw e;
            }
            catch (Throwable e) {
                U.error(log, "Failed to reinitialize local partitions (preloading will be stopped): " + exchId, e);

                onDone(e);

                if (e instanceof Error)
                    throw (Error)e;

                return;
            }

            if (F.isEmpty(rmtIds)) {
                onDone(exchId.topologyVersion());

                return;
            }

            ready.set(true);

            initFut.onDone(true);

            if (log.isDebugEnabled())
                log.debug("Initialized future: " + this);

            ClusterNode oldest = oldestNode.get();

            // If this node is not oldest.
            if (!oldest.id().equals(cctx.localNodeId()))
                sendPartitions(oldest);
            else {
                boolean allReceived = allReceived();

                if (allReceived && replied.compareAndSet(false, true)) {
                    if (spreadPartitions())
                        onDone(exchId.topologyVersion());
                }
            }

            scheduleRecheck();
        }
        else
            assert false : "Skipped init future: " + this;
    }

    /**
     * @return {@code True} if exchange initiated for client cache close.
     */
    private boolean clientCacheClose() {
        return reqs != null && reqs.size() == 1 && reqs.iterator().next().close();
    }

    /**
     * @param cacheId Cache ID to check.
     * @return {@code True} if cache is stopping by this exchange.
     */
    private boolean stopping(int cacheId) {
        boolean stopping = false;

        if (!F.isEmpty(reqs)) {
            for (DynamicCacheChangeRequest req : reqs) {
                if (cacheId == CU.cacheId(req.cacheName())) {
                    stopping = req.stop();

                    break;
                }
            }
        }

        return stopping;
    }

    /**
     * Starts dynamic caches.
     * @throws IgniteCheckedException If failed.
     */
    private void startCaches() throws IgniteCheckedException {
        cctx.cache().prepareCachesStart(F.view(reqs, new IgnitePredicate<DynamicCacheChangeRequest>() {
            @Override public boolean apply(DynamicCacheChangeRequest req) {
                return req.start();
            }
        }), exchId.topologyVersion());
    }

    /**
     *
     */
    private void blockGateways() {
        for (DynamicCacheChangeRequest req : reqs) {
            if (req.stop() || req.close())
                cctx.cache().blockGateway(req);
        }
    }

    /**
     * @param node Node.
     * @param id ID.
     * @throws IgniteCheckedException If failed.
     */
    private void sendLocalPartitions(ClusterNode node, @Nullable GridDhtPartitionExchangeId id)
        throws IgniteCheckedException {
        GridDhtPartitionsSingleMessage m = new GridDhtPartitionsSingleMessage(id,
            clientOnlyExchange,
            cctx.versions().last());

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal()) {
                GridDhtPartitionMap2 locMap = cacheCtx.topology().localPartitionMap();

                if (node.version().compareTo(GridDhtPartitionMap2.SINCE) < 0)
                    locMap = new GridDhtPartitionMap(locMap.nodeId(), locMap.updateSequence(), locMap.map());

                m.addLocalPartitionMap(cacheCtx.cacheId(), locMap);

                m.partitionUpdateCounters(cacheCtx.cacheId(), cacheCtx.topology().updateCounters());
            }
        }

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().send(node, m, SYSTEM_POOL);
    }

    /**
     * @param nodes Nodes.
     * @param id ID.
     * @throws IgniteCheckedException If failed.
     */
    private void sendAllPartitions(Collection<? extends ClusterNode> nodes, GridDhtPartitionExchangeId id)
        throws IgniteCheckedException {
        GridDhtPartitionsFullMessage m = new GridDhtPartitionsFullMessage(id,
            lastVer.get(),
            id.topologyVersion());

        boolean useOldApi = false;

        for (ClusterNode node : nodes) {
            if (node.version().compareTo(GridDhtPartitionMap2.SINCE) < 0)
                useOldApi = true;
        }

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal()) {
                AffinityTopologyVersion startTopVer = cacheCtx.startTopologyVersion();

                boolean ready = startTopVer == null || startTopVer.compareTo(id.topologyVersion()) <= 0;

                if (ready) {
                    GridDhtPartitionFullMap locMap = cacheCtx.topology().partitionMap(true);

                    if (useOldApi)
                        locMap = new GridDhtPartitionFullMap(locMap.nodeId(), locMap.nodeOrder(), locMap.updateSequence(), locMap);

                    m.addFullPartitionsMap(cacheCtx.cacheId(), locMap);

                    m.addPartitionUpdateCounters(cacheCtx.cacheId(), cacheCtx.topology().updateCounters());
                }
            }
        }

        // It is important that client topologies be added after contexts.
        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
            m.addFullPartitionsMap(top.cacheId(), top.partitionMap(true));

            m.addPartitionUpdateCounters(top.cacheId(), top.updateCounters());
        }

        if (log.isDebugEnabled())
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, SYSTEM_POOL, null);
    }

    /**
     * @param oldestNode Oldest node.
     */
    private void sendPartitions(ClusterNode oldestNode) {
        try {
            sendLocalPartitions(oldestNode, exchId);
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Oldest node left during partition exchange [nodeId=" + oldestNode.id() +
                    ", exchId=" + exchId + ']');
        }
        catch (IgniteCheckedException e) {
            scheduleRecheck();

            U.error(log, "Failed to send local partitions to oldest node (will retry after timeout) [oldestNodeId=" +
                oldestNode.id() + ", exchId=" + exchId + ']', e);
        }
    }

    /**
     * @return {@code True} if succeeded.
     */
    private boolean spreadPartitions() {
        try {
            sendAllPartitions(rmtNodes, exchId);

            return true;
        }
        catch (IgniteCheckedException e) {
            scheduleRecheck();

            if (!X.hasCause(e, InterruptedException.class))
                U.error(log, "Failed to send full partition map to nodes (will retry after timeout) [nodes=" +
                    F.nodeId8s(rmtNodes) + ", exchangeId=" + exchId + ']', e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(AffinityTopologyVersion res, Throwable err) {
        Map<Integer, Boolean> m = null;

        for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
            if (cacheCtx.config().getTopologyValidator() != null && !CU.isSystemCache(cacheCtx.name())) {
                if (m == null)
                    m = new HashMap<>();

                m.put(cacheCtx.cacheId(), cacheCtx.config().getTopologyValidator().validate(discoEvt.topologyNodes()));
            }
        }

        cacheValidRes = m != null ? m : Collections.<Integer, Boolean>emptyMap();

        cctx.cache().onExchangeDone(exchId.topologyVersion(), reqs, err);

        cctx.exchange().onExchangeDone(this, err);

        if (super.onDone(res, err) && !dummy && !forcePreload) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange= " + this +
                    "duration=" + duration() + ", durationFromInit=" + (U.currentTimeMillis() - initTs) + ']');

            initFut.onDone(err == null);

            GridTimeoutObject timeoutObj = this.timeoutObj;

            // Deschedule timeout object.
            if (timeoutObj != null)
                cctx.kernalContext().timeout().removeTimeoutObject(timeoutObj);

            if (exchId.isLeft()) {
                for (GridCacheContext cacheCtx : cctx.cacheContexts())
                    cacheCtx.config().getAffinity().removeNode(exchId.nodeId());
            }

            return true;
        }

        return dummy;
    }

    /** {@inheritDoc} */
    @Override public Throwable validateCache(GridCacheContext cctx) {
        Throwable err = error();

        if (err != null)
            return err;

        if (cctx.config().getTopologyValidator() != null) {
            Boolean res = cacheValidRes.get(cctx.cacheId());

            if (res != null && !res) {
                return new IgniteCheckedException("Failed to perform cache operation " +
                    "(cache topology is not valid): " + cctx.name());
            }
        }

        return null;
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        topSnapshot.set(null);
        singleMsgs.clear();
        fullMsgs.clear();
        rcvdIds.clear();
        oldestNode.set(null);
        partReleaseFut = null;

        Collection<ClusterNode> rmtNodes = this.rmtNodes;

        if (rmtNodes != null)
            rmtNodes.clear();
    }

    /**
     * @return {@code True} if all replies are received.
     */
    private boolean allReceived() {
        Collection<UUID> rmtIds = this.rmtIds;

        assert rmtIds != null : "Remote Ids can't be null: " + this;

        synchronized (rcvdIds) {
            return rcvdIds.containsAll(rmtIds);
        }
    }

    /**
     * @param nodeId Sender node id.
     * @param msg Single partition info.
     */
    public void onReceive(final UUID nodeId, final GridDhtPartitionsSingleMessage msg) {
        assert msg != null;

        assert msg.exchangeId().equals(exchId);

        // Update last seen version.
        while (true) {
            GridCacheVersion old = lastVer.get();

            if (old == null || old.compareTo(msg.lastVersion()) < 0) {
                if (lastVer.compareAndSet(old, msg.lastVersion()))
                    break;
            }
            else
                break;
        }

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future (will reply only to sender) [msg=" + msg +
                    ", fut=" + this + ']');

            sendAllPartitions(nodeId, cctx.gridConfig().getNetworkSendRetryCount());
        }
        else {
            initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> f) {
                    try {
                        if (!f.get())
                            return;
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to initialize exchange future: " + this, e);

                        return;
                    }

                    ClusterNode loc = cctx.localNode();

                    singleMsgs.put(nodeId, msg);

                    boolean match = true;

                    // Check if oldest node has changed.
                    if (!oldestNode.get().equals(loc)) {
                        match = false;

                        synchronized (mux) {
                            // Double check.
                            if (oldestNode.get().equals(loc))
                                match = true;
                        }
                    }

                    if (match) {
                        boolean allReceived;

                        synchronized (rcvdIds) {
                            if (rcvdIds.add(nodeId))
                                updatePartitionSingleMap(msg);

                            allReceived = allReceived();
                        }

                        // If got all replies, and initialization finished, and reply has not been sent yet.
                        if (allReceived && ready.get() && replied.compareAndSet(false, true)) {
                            spreadPartitions();

                            onDone(exchId.topologyVersion());
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Exchange future full map is not sent [allReceived=" + allReceived() +
                                ", ready=" + ready + ", replied=" + replied.get() + ", init=" + init.get() +
                                ", fut=" + GridDhtPartitionsExchangeFuture.this + ']');
                    }
                }
            });
        }
    }

    /**
     * @param nodeId Node ID.
     * @param retryCnt Number of retries.
     */
    private void sendAllPartitions(final UUID nodeId, final int retryCnt) {
        ClusterNode n = cctx.node(nodeId);

        try {
            if (n != null)
                sendAllPartitions(F.asList(n), exchId);
        }
        catch (IgniteCheckedException e) {
            if (e instanceof ClusterTopologyCheckedException || !cctx.discovery().alive(n)) {
                log.debug("Failed to send full partition map to node, node left grid " +
                    "[rmtNode=" + nodeId + ", exchangeId=" + exchId + ']');

                return;
            }

            if (retryCnt > 0) {
                long timeout = cctx.gridConfig().getNetworkSendRetryDelay();

                LT.error(log, e, "Failed to send full partition map to node (will retry after timeout) " +
                    "[node=" + nodeId + ", exchangeId=" + exchId + ", timeout=" + timeout + ']');

                cctx.time().addTimeoutObject(new GridTimeoutObjectAdapter(timeout) {
                    @Override public void onTimeout() {
                        sendAllPartitions(nodeId, retryCnt - 1);
                    }
                });
            }
            else
                U.error(log, "Failed to send full partition map [node=" + n + ", exchangeId=" + exchId + ']', e);
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Full partition info.
     */
    public void onReceive(final UUID nodeId, final GridDhtPartitionsFullMessage msg) {
        assert msg != null;

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future [msg=" + msg + ", fut=" + this + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Received full partition map from node [nodeId=" + nodeId + ", msg=" + msg + ']');

        assert exchId.topologyVersion().equals(msg.topologyVersion());

        initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
            @Override public void apply(IgniteInternalFuture<Boolean> f) {
                try {
                    if (!f.get())
                        return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to initialize exchange future: " + this, e);

                    return;
                }

                ClusterNode curOldest = oldestNode.get();

                if (!nodeId.equals(curOldest.id())) {
                    if (log.isDebugEnabled())
                        log.debug("Received full partition map from unexpected node [oldest=" + curOldest.id() +
                            ", unexpectedNodeId=" + nodeId + ']');

                    ClusterNode snd = cctx.discovery().node(nodeId);

                    if (snd == null) {
                        if (log.isDebugEnabled())
                            log.debug("Sender node left grid, will ignore message from unexpected node [nodeId=" + nodeId +
                                ", exchId=" + msg.exchangeId() + ']');

                        return;
                    }

                    // Will process message later if sender node becomes oldest node.
                    if (snd.order() > curOldest.order())
                        fullMsgs.put(nodeId, msg);

                    return;
                }

                assert msg.exchangeId().equals(exchId);

                assert msg.lastVersion() != null;

                cctx.versions().onReceived(nodeId, msg.lastVersion());

                updatePartitionFullMap(msg);

                onDone(exchId.topologyVersion());
            }
        });
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions full messages.
     */
    private void updatePartitionFullMap(GridDhtPartitionsFullMessage msg) {
        for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();

            Map<Integer, Long> cntrMap = msg.partitionUpdateCounters(cacheId);

            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx != null)
                cacheCtx.topology().update(exchId, entry.getValue(), cntrMap);
            else {
                ClusterNode oldest = CU.oldestAliveCacheServerNode(cctx, AffinityTopologyVersion.NONE);

                if (oldest != null && oldest.isLocal())
                    cctx.exchange().clientTopology(cacheId, this).update(exchId, entry.getValue(), cntrMap);
            }
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions single message.
     */
    private void updatePartitionSingleMap(GridDhtPartitionsSingleMessage msg) {
        for (Map.Entry<Integer, GridDhtPartitionMap2> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();
            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

            GridDhtPartitionTopology top = cacheCtx != null ? cacheCtx.topology() :
                cctx.exchange().clientTopology(cacheId, this);

            top.update(exchId, entry.getValue(), msg.partitionUpdateCounters(cacheId));
        }
    }

    /**
     * @param nodeId Left node id.
     */
    public void onNodeLeft(final UUID nodeId) {
        if (isDone())
            return;

        if (!enterBusy())
            return;

        try {
            // Wait for initialization part of this future to complete.
            initFut.listen(new CI1<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> f) {
                    try {
                        if (!f.get())
                            return;
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to initialize exchange future: " + this, e);

                        return;
                    }

                    if (isDone())
                        return;

                    if (!enterBusy())
                        return;

                    try {
                        // Pretend to have received message from this node.
                        rcvdIds.add(nodeId);

                        Collection<UUID> rmtIds = GridDhtPartitionsExchangeFuture.this.rmtIds;

                        assert rmtIds != null;

                        ClusterNode oldest = oldestNode.get();

                        if (oldest.id().equals(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Oldest node left or failed on partition exchange " +
                                    "(will restart exchange process)) [oldestNodeId=" + oldest.id() +
                                    ", exchangeId=" + exchId + ']');

                            boolean set = false;

                            ClusterNode newOldest = CU.oldestAliveCacheServerNode(cctx, exchId.topologyVersion());

                            if (newOldest != null) {
                                // If local node is now oldest.
                                if (newOldest.id().equals(cctx.localNodeId())) {
                                    synchronized (mux) {
                                        if (oldestNode.compareAndSet(oldest, newOldest)) {
                                            // If local node is just joining.
                                            if (exchId.nodeId().equals(cctx.localNodeId())) {
                                                try {
                                                    for (GridCacheContext cacheCtx : cctx.cacheContexts()) {
                                                        if (!cacheCtx.isLocal())
                                                            cacheCtx.topology().beforeExchange(
                                                                GridDhtPartitionsExchangeFuture.this);
                                                    }
                                                }
                                                catch (IgniteCheckedException e) {
                                                    onDone(e);

                                                    return;
                                                }
                                            }

                                            set = true;
                                        }
                                    }
                                }
                                else {
                                    synchronized (mux) {
                                        set = oldestNode.compareAndSet(oldest, newOldest);
                                    }

                                    if (set && log.isDebugEnabled())
                                        log.debug("Reassigned oldest node [this=" + cctx.localNodeId() +
                                            ", old=" + oldest.id() + ", new=" + newOldest.id() + ']');
                                }
                            }
                            else {
                                ClusterTopologyCheckedException err = new ClusterTopologyCheckedException("Failed to " +
                                    "wait for exchange future, all server nodes left.");

                                onDone(err);
                            }

                            if (set) {
                                // If received any messages, process them.
                                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage> m : singleMsgs.entrySet())
                                    onReceive(m.getKey(), m.getValue());

                                for (Map.Entry<UUID, GridDhtPartitionsFullMessage> m : fullMsgs.entrySet())
                                    onReceive(m.getKey(), m.getValue());

                                // Reassign oldest node and resend.
                                recheck();
                            }
                        }
                        else if (rmtIds.contains(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Remote node left of failed during partition exchange (will ignore) " +
                                    "[rmtNode=" + nodeId + ", exchangeId=" + exchId + ']');

                            assert rmtNodes != null;

                            for (Iterator<ClusterNode> it = rmtNodes.iterator(); it.hasNext(); ) {
                                if (it.next().id().equals(nodeId))
                                    it.remove();
                            }

                            if (allReceived() && ready.get() && replied.compareAndSet(false, true))
                                if (spreadPartitions())
                                    onDone(exchId.topologyVersion());
                        }
                    }
                    finally {
                        leaveBusy();
                    }
                }
            });
        }
        finally {
            leaveBusy();
        }
    }

    /**
     *
     */
    private void recheck() {
        ClusterNode oldest = oldestNode.get();

        // If this is the oldest node.
        if (oldest.id().equals(cctx.localNodeId())) {
            Collection<UUID> remaining = remaining();

            if (!remaining.isEmpty()) {
                try {
                    cctx.io().safeSend(cctx.discovery().nodes(remaining),
                        new GridDhtPartitionsSingleRequest(exchId), SYSTEM_POOL, null);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to request partitions from nodes [exchangeId=" + exchId +
                        ", nodes=" + remaining + ']', e);
                }
            }
            // Resend full partition map because last attempt failed.
            else {
                if (spreadPartitions())
                    onDone(exchId.topologyVersion());
            }
        }
        else
            sendPartitions(oldest);

        // Schedule another send.
        scheduleRecheck();
    }

    /**
     *
     */
    private void scheduleRecheck() {
        if (!isDone()) {
            GridTimeoutObject old = timeoutObj;

            if (old != null)
                cctx.kernalContext().timeout().removeTimeoutObject(old);

            GridTimeoutObject timeoutObj = new GridTimeoutObjectAdapter(
                cctx.gridConfig().getNetworkTimeout() * Math.max(1, cctx.gridConfig().getCacheConfiguration().length)) {
                @Override public void onTimeout() {
                    cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                        @Override public void run() {
                            if (isDone())
                                return;

                            if (!enterBusy())
                                return;

                            try {
                                U.warn(log,
                                    "Retrying preload partition exchange due to timeout [done=" + isDone() +
                                        ", dummy=" + dummy + ", exchId=" + exchId + ", rcvdIds=" + F.id8s(rcvdIds) +
                                        ", rmtIds=" + F.id8s(rmtIds) + ", remaining=" + F.id8s(remaining()) +
                                        ", init=" + init + ", initFut=" + initFut.isDone() +
                                        ", ready=" + ready + ", replied=" + replied + ", added=" + added +
                                        ", oldest=" + U.id8(oldestNode.get().id()) + ", oldestOrder=" +
                                        oldestNode.get().order() + ", evtLatch=" + evtLatch.getCount() +
                                        ", locNodeOrder=" + cctx.localNode().order() +
                                        ", locNodeId=" + cctx.localNode().id() + ']',
                                    "Retrying preload partition exchange due to timeout.");

                                recheck();
                            }
                            finally {
                                leaveBusy();
                            }
                        }
                    });
                }
            };

            this.timeoutObj = timeoutObj;

            cctx.kernalContext().timeout().addTimeoutObject(timeoutObj);
        }
    }

    /**
     * @return Remaining node IDs.
     */
    Collection<UUID> remaining() {
        if (rmtIds == null)
            return Collections.emptyList();

        return F.lose(rmtIds, true, rcvdIds);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridDhtPartitionsExchangeFuture fut) {
        return exchId.compareTo(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridDhtPartitionsExchangeFuture fut = (GridDhtPartitionsExchangeFuture)o;

        return exchId.equals(fut.exchId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return exchId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        ClusterNode oldestNode = this.oldestNode.get();

        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "oldest", oldestNode == null ? "null" : oldestNode.id(),
            "oldestOrder", oldestNode == null ? "null" : oldestNode.order(),
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "remaining", remaining(),
            "super", super.toString());
    }
}
