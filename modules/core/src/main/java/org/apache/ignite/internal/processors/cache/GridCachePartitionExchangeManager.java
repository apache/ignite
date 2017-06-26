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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNeedReconnectException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.events.DiscoveryCustomEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.pagemem.snapshot.StartFullSnapshotAckDiscoveryMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridClientPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.schema.SchemaNodeLeaveExchangeWorkerTask;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.GridListSet;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_IO_DUMP_ON_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PRELOAD_RESEND_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture.nextDumpTimeout;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader.DFLT_PRELOAD_RESEND_TIMEOUT;

/**
 * Partition exchange manager.
 */
public class GridCachePartitionExchangeManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Exchange history size. */
    private static final int EXCHANGE_HISTORY_SIZE =
        IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE, 1_000);

    /** Atomic reference for pending timeout object. */
    private AtomicReference<ResendTimeoutObject> pendingResend = new AtomicReference<>();

    /** Partition resend timeout after eviction. */
    private final long partResendTimeout = getLong(IGNITE_PRELOAD_RESEND_TIMEOUT, DFLT_PRELOAD_RESEND_TIMEOUT);

    /** */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Last partition refresh. */
    private final AtomicLong lastRefresh = new AtomicLong(-1);

    /** */
    @GridToStringInclude
    private ExchangeWorker exchWorker;

    /** */
    @GridToStringExclude
    private final ConcurrentMap<Integer, GridClientPartitionTopology> clientTops = new ConcurrentHashMap8<>();

    /** */
    private volatile GridDhtPartitionsExchangeFuture lastInitializedFut;

    /** */
    private final AtomicReference<GridDhtTopologyFuture> lastFinishedFut = new AtomicReference<>();

    /** */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentSkipListMap<AffinityTopologyVersion, IgnitePair<IgniteProductVersion>> nodeVers =
        new ConcurrentSkipListMap<>();

    /** */
    private final AtomicReference<AffinityTopologyVersion> readyTopVer =
        new AtomicReference<>(AffinityTopologyVersion.NONE);

    /** */
    private GridFutureAdapter<?> reconnectExchangeFut;

    /** */
    private final Object interruptLock = new Object();

    /**
     * Partition map futures.
     * This set also contains already completed exchange futures to address race conditions when coordinator
     * leaves grid and new coordinator sends full partition message to a node which has not yet received
     * discovery event. In case if remote node will retry partition exchange, completed future will indicate
     * that full partition map should be sent to requesting node right away.
     */
    private ExchangeFutureSet exchFuts = new ExchangeFutureSet();

    /** */
    private volatile IgniteCheckedException stopErr;

    /** */
    private long nextLongRunningOpsDumpTime;

    /** */
    private int longRunningOpsDumpStep;

    /** */
    private DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

    /** Discovery listener. */
    private final DiscoveryEventListener discoLsnr = new DiscoveryEventListener() {
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache cache) {
            if (!enterBusy())
                return;

            try {
                ClusterNode loc = cctx.localNode();

                assert evt.type() == EVT_NODE_JOINED || evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED ||
                    evt.type() == EVT_DISCOVERY_CUSTOM_EVT;

                final ClusterNode n = evt.eventNode();

                GridDhtPartitionExchangeId exchId = null;
                GridDhtPartitionsExchangeFuture exchFut = null;

                if (evt.type() != EVT_DISCOVERY_CUSTOM_EVT) {
                    assert !loc.id().equals(n.id());

                    if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED) {
                        assert cctx.discovery().node(n.id()) == null;

                        // Avoid race b/w initial future add and discovery event.
                        GridDhtPartitionsExchangeFuture initFut = null;

                        if (readyTopVer.get().equals(AffinityTopologyVersion.NONE)) {
                            initFut = exchangeFuture(initialExchangeId(), null, null, null, null);

                            initFut.onNodeLeft(n);
                        }

                        for (GridDhtPartitionsExchangeFuture f : exchFuts.values()) {
                            if (f != initFut)
                                f.onNodeLeft(n);
                        }
                    }

                    assert evt.type() != EVT_NODE_JOINED || n.order() > loc.order() :
                        "Node joined with smaller-than-local " +
                        "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ']';

                    exchId = exchangeId(n.id(),
                        affinityTopologyVersion(evt),
                        evt.type());

                    exchFut = exchangeFuture(exchId, evt, cache,null, null);
                }
                else {
                    DiscoveryCustomMessage customMsg = ((DiscoveryCustomEvent)evt).customMessage();

                    if (customMsg instanceof DynamicCacheChangeBatch) {
                        DynamicCacheChangeBatch batch = (DynamicCacheChangeBatch)customMsg;

                        ExchangeActions exchActions = batch.exchangeActions();

                        if (exchActions != null) {
                            exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt.type());

                            exchFut = exchangeFuture(exchId, evt, cache, exchActions, null);
                        }
                    }
                    else if (customMsg instanceof CacheAffinityChangeMessage) {
                        CacheAffinityChangeMessage msg = (CacheAffinityChangeMessage)customMsg;

                        if (msg.exchangeId() == null) {
                            if (msg.exchangeNeeded()) {
                                exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt.type());

                                exchFut = exchangeFuture(exchId, evt, cache, null, msg);
                            }
                        }
                        else {
                            exchangeFuture(msg.exchangeId(), null, null, null, null)
                                .onAffinityChangeMessage(evt.eventNode(), msg);
                        }
                    }
                    else if (customMsg instanceof StartFullSnapshotAckDiscoveryMessage
                        && !((StartFullSnapshotAckDiscoveryMessage)customMsg).hasError()) {
                        exchId = exchangeId(n.id(), affinityTopologyVersion(evt), evt.type());

                        exchFut = exchangeFuture(exchId, evt, null, null, null);
                    }
                    else {
                        // Process event as custom discovery task if needed.
                        CachePartitionExchangeWorkerTask task =
                            cctx.cache().exchangeTaskForCustomDiscoveryMessage(customMsg);

                        if (task != null)
                            exchWorker.addCustomTask(task);
                    }
                }

                if (exchId != null) {
                    if (log.isDebugEnabled())
                        log.debug("Discovery event (will start exchange): " + exchId);

                    // Event callback - without this callback future will never complete.
                    exchFut.onEvent(exchId, evt, cache);

                    // Start exchange process.
                    addFuture(exchFut);
                }
                else {
                    if (log.isDebugEnabled())
                        log.debug("Do not start exchange for discovery event: " + evt);
                }

                // Notify indexing engine about node leave so that we can re-map coordinator accordingly.
                if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                    exchWorker.addCustomTask(new SchemaNodeLeaveExchangeWorkerTask(evt.eventNode()));
            }
            finally {
                leaveBusy();
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        exchWorker = new ExchangeWorker();

        cctx.gridEvents().addDiscoveryEventListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED,
            EVT_DISCOVERY_CUSTOM_EVT);

        cctx.io().addCacheHandler(0, GridDhtPartitionsSingleMessage.class,
            new MessageHandler<GridDhtPartitionsSingleMessage>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
                    processSinglePartitionUpdate(node, msg);
                }
            });

        cctx.io().addCacheHandler(0, GridDhtPartitionsFullMessage.class,
            new MessageHandler<GridDhtPartitionsFullMessage>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsFullMessage msg) {
                    processFullPartitionUpdate(node, msg);
                }
            });

        cctx.io().addCacheHandler(0, GridDhtPartitionsSingleRequest.class,
            new MessageHandler<GridDhtPartitionsSingleRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
                    processSinglePartitionRequest(node, msg);
                }
            });
    }

    /**
     * @param task Task to run in exchange worker thread.
     */
    public void addCustomTask(CachePartitionExchangeWorkerTask task) {
        assert task != null;

        exchWorker.addCustomTask(task);
    }

    /**
     * @return Reconnect partition exchange future.
     */
    public IgniteInternalFuture<?> reconnectExchangeFuture() {
        return reconnectExchangeFut;
    }

    /**
     * @return Initial exchange ID.
     */
    private GridDhtPartitionExchangeId initialExchangeId() {
        DiscoveryEvent discoEvt = cctx.discovery().localJoinEvent();

        assert discoEvt != null;

        final AffinityTopologyVersion startTopVer = affinityTopologyVersion(discoEvt);

        assert discoEvt.topologyVersion() == startTopVer.topologyVersion();

        return exchangeId(cctx.localNode().id(), startTopVer, EVT_NODE_JOINED);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        super.onKernalStart0(reconnect);

        ClusterNode loc = cctx.localNode();

        long startTime = loc.metrics().getStartTime();

        assert startTime > 0;

        // Generate dummy discovery event for local node joining.
        T2<DiscoveryEvent, DiscoCache> locJoin = cctx.discovery().localJoin();

        DiscoveryEvent discoEvt = locJoin.get1();
        DiscoCache discoCache = locJoin.get2();

        GridDhtPartitionExchangeId exchId = initialExchangeId();

        GridDhtPartitionsExchangeFuture fut = exchangeFuture(exchId, discoEvt, discoCache, null, null);

        if (reconnect)
            reconnectExchangeFut = new GridFutureAdapter<>();

        exchWorker.addFirstExchangeFuture(fut);

        if (!cctx.kernalContext().clientNode()) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++) {
                final int idx = cnt;

                cctx.io().addOrderedCacheGroupHandler(rebalanceTopic(cnt), new CI2<UUID, GridCacheGroupIdMessage>() {
                    @Override public void apply(final UUID id, final GridCacheGroupIdMessage m) {
                        if (!enterBusy())
                            return;

                        try {
                            CacheGroupContext grp = cctx.cache().cacheGroup(m.groupId());

                            if (grp != null) {
                                if (m instanceof GridDhtPartitionSupplyMessage) {
                                    grp.preloader().handleSupplyMessage(idx, id, (GridDhtPartitionSupplyMessage) m);

                                    return;
                                }
                                else if (m instanceof GridDhtPartitionDemandMessage) {
                                    grp.preloader().handleDemandMessage(idx, id, (GridDhtPartitionDemandMessage) m);

                                    return;
                                }
                            }

                            U.error(log, "Unsupported message type: " + m.getClass().getName());
                        }
                        finally {
                            leaveBusy();
                        }
                    }
                });
            }
        }

        new IgniteThread(cctx.igniteInstanceName(), "exchange-worker", exchWorker).start();

        if (reconnect) {
            fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    try {
                        fut.get();

                        for (CacheGroupContext grp : cctx.cache().cacheGroups())
                            grp.preloader().onInitialExchangeComplete(null);

                        reconnectExchangeFut.onDone();
                    }
                    catch (IgniteCheckedException e) {
                        for (CacheGroupContext grp : cctx.cache().cacheGroups())
                            grp.preloader().onInitialExchangeComplete(e);

                        reconnectExchangeFut.onDone(e);
                    }
                }
            });
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Beginning to wait on local exchange future: " + fut);

            boolean first = true;

            while (true) {
                try {
                    fut.get(cctx.preloadExchangeTimeout());

                    break;
                }
                catch (IgniteFutureTimeoutCheckedException ignored) {
                    if (first) {
                        U.warn(log, "Failed to wait for initial partition map exchange. " +
                            "Possible reasons are: " + U.nl() +
                            "  ^-- Transactions in deadlock." + U.nl() +
                            "  ^-- Long running transactions (ignore if this is the case)." + U.nl() +
                            "  ^-- Unreleased explicit locks.");

                        first = false;
                    }
                    else
                        U.warn(log, "Still waiting for initial partition map exchange [fut=" + fut + ']');
                }
                catch (IgniteNeedReconnectException e) {
                    throw e;
                }
                catch (Exception e) {
                    if (fut.reconnectOnError(e))
                        throw new IgniteNeedReconnectException(cctx.localNode(), e);

                    throw e;
                }
            }

            AffinityTopologyVersion nodeStartVer = new AffinityTopologyVersion(discoEvt.topologyVersion(), 0);

            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (nodeStartVer.equals(grp.localStartVersion()))
                    grp.preloader().onInitialExchangeComplete(null);
            }

            if (log.isDebugEnabled())
                log.debug("Finished waiting for initial exchange: " + fut.exchangeId());
        }
    }

    /**
     * @param idx Index.
     * @return Topic for index.
     */
    public static Object rebalanceTopic(int idx) {
        return TOPIC_CACHE.topic("Rebalance", idx);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        cctx.gridEvents().removeDiscoveryEventListener(discoLsnr);

        cctx.io().removeHandler(false, 0, GridDhtPartitionsSingleMessage.class);
        cctx.io().removeHandler(false, 0, GridDhtPartitionsFullMessage.class);
        cctx.io().removeHandler(false, 0, GridDhtPartitionsSingleRequest.class);

        stopErr = cctx.kernalContext().clientDisconnected() ?
            new IgniteClientDisconnectedCheckedException(cctx.kernalContext().cluster().clientReconnectFuture(),
                "Client node disconnected: " + cctx.igniteInstanceName()) :
            new IgniteInterruptedCheckedException("Node is stopping: " + cctx.igniteInstanceName());

        // Finish all exchange futures.
        ExchangeFutureSet exchFuts0 = exchFuts;

        if (exchFuts0 != null) {
            for (GridDhtPartitionsExchangeFuture f : exchFuts.values())
                f.onDone(stopErr);
        }

        for (AffinityReadyFuture f : readyFuts.values())
            f.onDone(stopErr);

        if (!cctx.kernalContext().clientNode()) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++)
                cctx.io().removeOrderedHandler(true, rebalanceTopic(cnt));
        }

        U.cancel(exchWorker);

        if (log.isDebugEnabled())
            log.debug("Before joining on exchange worker: " + exchWorker);

        U.join(exchWorker, log);

        ResendTimeoutObject resendTimeoutObj = pendingResend.getAndSet(null);

        if (resendTimeoutObj != null)
            cctx.time().removeTimeoutObject(resendTimeoutObj);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        // Do not allow any activity in exchange manager after stop.
        busyLock.writeLock().lock();

        exchFuts = null;
    }

    /**
     * @return Interrupt lock.
     */
    public Object interruptLock() {
        return interruptLock;
    }

    /**
     * @param grpId Cache group ID.
     * @param exchFut Exchange future.
     * @return Topology.
     */
    public GridDhtPartitionTopology clientTopology(int grpId, GridDhtPartitionsExchangeFuture exchFut) {
        GridClientPartitionTopology top = clientTops.get(grpId);

        if (top != null)
            return top;

        Object affKey = null;

        CacheGroupDescriptor grpDesc = cctx.cache().cacheGroupDescriptors().get(grpId);

        if (grpDesc != null) {
            CacheConfiguration<?, ?> ccfg = grpDesc.config();

            AffinityFunction aff = ccfg.getAffinity();

            affKey = cctx.kernalContext().affinity().similaryAffinityKey(aff,
                ccfg.getNodeFilter(),
                ccfg.getBackups(),
                aff.partitions());
        }

        GridClientPartitionTopology old = clientTops.putIfAbsent(grpId,
            top = new GridClientPartitionTopology(cctx, grpId, exchFut, affKey));

        return old != null ? old : top;
    }

    /**
     * @return Collection of client topologies.
     */
    public Collection<GridClientPartitionTopology> clientTopologies() {
        return clientTops.values();
    }

    /**
     * @param grpId Cache group ID.
     * @return Client partition topology.
     */
    public GridClientPartitionTopology clearClientTopology(int grpId) {
        return clientTops.remove(grpId);
    }

    /**
     * Gets topology version of last partition exchange, it is possible that last partition exchange
     * is not completed yet.
     *
     * @return Topology version.
     */
    public AffinityTopologyVersion topologyVersion() {
        GridDhtPartitionsExchangeFuture lastInitializedFut0 = lastInitializedFut;

        return lastInitializedFut0 != null
            ? lastInitializedFut0.exchangeId().topologyVersion() : AffinityTopologyVersion.NONE;
    }

    /**
     * @return Topology version of latest completed partition exchange.
     */
    public AffinityTopologyVersion readyAffinityVersion() {
        return readyTopVer.get();
    }

    /**
     * @return Last initialized topology future.
     */
    public GridDhtTopologyFuture lastTopologyFuture() {
        return lastInitializedFut;
    }

    /**
     * @return Last finished topology future.
     */
    @Nullable public GridDhtTopologyFuture lastFinishedFuture() {
        return lastFinishedFut.get();
    }

    /**
     * @param fut Finished future.
     */
    public void lastFinishedFuture(GridDhtTopologyFuture fut) {
        assert fut != null && fut.isDone() : fut;

        while (true) {
            GridDhtTopologyFuture cur = lastFinishedFut.get();

            if (cur == null || fut.topologyVersion().compareTo(cur.topologyVersion()) > 0) {
                if (lastFinishedFut.compareAndSet(cur, fut))
                    break;
            }
            else
                break;
        }
    }

    /**
     * @param ver Topology version.
     * @return Future or {@code null} is future is already completed.
     */
    @Nullable public IgniteInternalFuture<?> affinityReadyFuture(AffinityTopologyVersion ver) {
        GridDhtPartitionsExchangeFuture lastInitializedFut0 = lastInitializedFut;

        if (lastInitializedFut0 != null && lastInitializedFut0.topologyVersion().compareTo(ver) == 0) {
            if (log.isDebugEnabled())
                log.debug("Return lastInitializedFut for topology ready future " +
                    "[ver=" + ver + ", fut=" + lastInitializedFut0 + ']');

            return lastInitializedFut0;
        }

        AffinityTopologyVersion topVer = readyTopVer.get();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Return finished future for topology ready future [ver=" + ver + ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, ver,
            new AffinityReadyFuture(ver));

        if (log.isDebugEnabled())
            log.debug("Created topology ready future [ver=" + ver + ", fut=" + fut + ']');

        topVer = readyTopVer.get();

        if (topVer.compareTo(ver) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Completing created topology ready future " +
                    "[ver=" + topVer + ", topVer=" + topVer + ", fut=" + fut + ']');

            fut.onDone(topVer);
        }
        else if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * Gets minimum node version for the given topology version.
     *
     * @param topVer Topology version to get minimum node version for.
     * @return Minimum node version.
     */
    public IgniteProductVersion minimumNodeVersion(AffinityTopologyVersion topVer) {
        IgnitePair<IgniteProductVersion> vers = nodeVers.get(topVer);

        return vers == null ? cctx.localNode().version() : vers.get1();
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (exchange manager is stopping): " + cctx.localNodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @return Exchange futures.
     */
    public List<GridDhtPartitionsExchangeFuture> exchangeFutures() {
        return exchFuts.values();
    }

    /**
     * @return {@code True} if pending future queue is empty.
     */
    public boolean hasPendingExchange() {
        return exchWorker.hasPendingExchange();
    }

    /**
     * @param evt Discovery event.
     * @return Affinity topology version.
     */
    private AffinityTopologyVersion affinityTopologyVersion(DiscoveryEvent evt) {
        if (evt.type() == DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT)
            return ((DiscoveryCustomEvent)evt).affinityTopologyVersion();

        return new AffinityTopologyVersion(evt.topologyVersion());
    }

    /**
     * @param exchFut Exchange future.
     * @param reassign Dummy reassign flag.
     */
    public void forceDummyExchange(boolean reassign,
        GridDhtPartitionsExchangeFuture exchFut) {
        exchWorker.addExchangeFuture(
            new GridDhtPartitionsExchangeFuture(cctx, reassign, exchFut.discoveryEvent(), exchFut.exchangeId()));
    }

    /**
     * Forces preload exchange.
     *
     * @param exchFut Exchange future.
     */
    public IgniteInternalFuture<Boolean> forceRebalance(GridDhtPartitionsExchangeFuture exchFut) {
        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(CU.boolReducer());

        exchWorker.addExchangeFuture(
            new GridDhtPartitionsExchangeFuture(cctx, exchFut.discoveryEvent(), exchFut.exchangeId(), fut));

        return fut;
    }

    /**
     * Schedules next full partitions update.
     */
    public void scheduleResendPartitions() {
        ResendTimeoutObject timeout = pendingResend.get();

        if (timeout == null || timeout.started()) {
            ResendTimeoutObject update = new ResendTimeoutObject();

            if (pendingResend.compareAndSet(timeout, update))
                cctx.time().addTimeoutObject(update);
        }
    }

    /**
     * Partition refresh callback.
     */
    private void refreshPartitions() {
        ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(AffinityTopologyVersion.NONE);

        if (oldest == null) {
            if (log.isDebugEnabled())
                log.debug("Skip partitions refresh, there are no server nodes [loc=" + cctx.localNodeId() + ']');

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Refreshing partitions [oldest=" + oldest.id() + ", loc=" + cctx.localNodeId() + ']');

        // If this is the oldest node.
        if (oldest.id().equals(cctx.localNodeId())) {
            // Check rebalance state & send CacheAffinityChangeMessage if need.
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (!grp.isLocal()) {
                    GridDhtPartitionTopology top = grp.topology();

                    if (top != null)
                        cctx.affinity().checkRebalanceState(top, grp.groupId());
                }
            }

            GridDhtPartitionsExchangeFuture lastFut = lastInitializedFut;

            // No need to send to nodes which did not finish their first exchange.
            AffinityTopologyVersion rmtTopVer =
                lastFut != null ? lastFut.topologyVersion() : AffinityTopologyVersion.NONE;

            Collection<ClusterNode> rmts = CU.remoteNodes(cctx, rmtTopVer);

            if (log.isDebugEnabled())
                log.debug("Refreshing partitions from oldest node: " + cctx.localNodeId());

            sendAllPartitions(rmts);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Refreshing local partitions from non-oldest node: " +
                    cctx.localNodeId());

            sendLocalPartitions(oldest, null);
        }
    }

    /**
     * @param nodes Nodes.
     */
    private void sendAllPartitions(Collection<ClusterNode> nodes) {
        GridDhtPartitionsFullMessage m = createPartitionsFullMessage(nodes, null, null, true);

        if (log.isDebugEnabled())
            log.debug("Sending all partitions [nodeIds=" + U.nodeIds(nodes) + ", msg=" + m + ']');

        for (ClusterNode node : nodes) {
            try {
                assert !node.equals(cctx.localNode());

                cctx.io().sendNoRetry(node, m, SYSTEM_POOL);
            }
            catch (ClusterTopologyCheckedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send partition update to node because it left grid (will ignore) [node=" +
                        node.id() + ", msg=" + m + ']');
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to send partitions full message [node=" + node + ", err=" + e + ']');
            }
        }
    }

    /**
     * @param nodes Target nodes.
     * @param exchId Non-null exchange ID if message is created for exchange.
     * @param lastVer Last version.
     * @param compress {@code True} if it is possible to use compression for message.
     * @return Message.
     */
    public GridDhtPartitionsFullMessage createPartitionsFullMessage(Collection<ClusterNode> nodes,
        final @Nullable GridDhtPartitionExchangeId exchId,
        @Nullable GridCacheVersion lastVer,
        final boolean compress) {
        final GridDhtPartitionsFullMessage m = new GridDhtPartitionsFullMessage(exchId,
            lastVer,
            exchId != null ? exchId.topologyVersion() : AffinityTopologyVersion.NONE);

        m.compress(compress);

        final Map<Object, T2<Integer, GridDhtPartitionFullMap>> dupData = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!grp.isLocal()) {
                if (exchId != null) {
                    AffinityTopologyVersion startTopVer = grp.localStartVersion();

                    if (startTopVer.compareTo(exchId.topologyVersion()) > 0)
                        continue;
                }

                GridAffinityAssignmentCache affCache = grp.affinity();

                GridDhtPartitionFullMap locMap = grp.topology().partitionMap(true);

                if (locMap != null) {
                    addFullPartitionsMap(m,
                        dupData,
                        compress,
                        grp.groupId(),
                        locMap,
                        affCache.similarAffinityKey());
                }

                if (exchId != null)
                    m.addPartitionUpdateCounters(grp.groupId(), grp.topology().updateCounters(true));
            }
        }

        // It is important that client topologies be added after contexts.
        for (GridClientPartitionTopology top : cctx.exchange().clientTopologies()) {
            GridDhtPartitionFullMap map = top.partitionMap(true);

            if (map != null) {
                addFullPartitionsMap(m,
                    dupData,
                    compress,
                    top.groupId(),
                    map,
                    top.similarAffinityKey());
            }

            if (exchId != null)
                m.addPartitionUpdateCounters(top.groupId(), top.updateCounters(true));
        }

        return m;
    }

    /**
     * @param m Message.
     * @param dupData Duplicated data map.
     * @param compress {@code True} if need check for duplicated partition state data.
     * @param grpId Cache group ID.
     * @param map Map to add.
     * @param affKey Cache affinity key.
     */
    private void addFullPartitionsMap(GridDhtPartitionsFullMessage m,
        Map<Object, T2<Integer, GridDhtPartitionFullMap>> dupData,
        boolean compress,
        Integer grpId,
        GridDhtPartitionFullMap map,
        Object affKey) {
        assert map != null;

        Integer dupDataCache = null;

        if (compress && affKey != null && !m.containsGroup(grpId)) {
            T2<Integer, GridDhtPartitionFullMap> state0 = dupData.get(affKey);

            if (state0 != null && state0.get2().partitionStateEquals(map)) {
                GridDhtPartitionFullMap map0 = new GridDhtPartitionFullMap(map.nodeId(),
                    map.nodeOrder(),
                    map.updateSequence());

                for (Map.Entry<UUID, GridDhtPartitionMap> e : map.entrySet())
                    map0.put(e.getKey(), e.getValue().emptyCopy());

                map = map0;

                dupDataCache = state0.get1();
            }
            else
                dupData.put(affKey, new T2<>(grpId, map));
        }

        m.addFullPartitionsMap(grpId, map, dupDataCache);
    }

    /**
     * @param node Node.
     * @param id ID.
     */
    private void sendLocalPartitions(ClusterNode node, @Nullable GridDhtPartitionExchangeId id) {
        GridDhtPartitionsSingleMessage m = createPartitionsSingleMessage(node,
            id,
            cctx.kernalContext().clientNode(),
            false);

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", msg=" + m + ']');

        try {
            cctx.io().sendNoRetry(node, m, SYSTEM_POOL);
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partition update to node because it left grid (will ignore) [node=" +
                    node.id() + ", msg=" + m + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send local partition map to node [node=" + node + ", exchId=" + id + ']', e);
        }
    }

    /**
     * @param targetNode Target node.
     * @param exchangeId ID.
     * @param clientOnlyExchange Client exchange flag.
     * @param sndCounters {@code True} if need send partition update counters.
     * @return Message.
     */
    public GridDhtPartitionsSingleMessage createPartitionsSingleMessage(ClusterNode targetNode,
        @Nullable GridDhtPartitionExchangeId exchangeId,
        boolean clientOnlyExchange,
        boolean sndCounters)
    {
        GridDhtPartitionsSingleMessage m = new GridDhtPartitionsSingleMessage(exchangeId,
            clientOnlyExchange,
            cctx.versions().last(),
            true);

        Map<Object, T2<Integer,Map<Integer, GridDhtPartitionState>>> dupData = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (!grp.isLocal()) {
                GridDhtPartitionMap locMap = grp.topology().localPartitionMap();

                addPartitionMap(m,
                    dupData,
                    true,
                    grp.groupId(),
                    locMap,
                    grp.affinity().similarAffinityKey());

                if (sndCounters)
                    m.partitionUpdateCounters(grp.groupId(), grp.topology().updateCounters(true));
            }
        }

        for (GridClientPartitionTopology top : clientTops.values()) {
            if (m.partitions() != null && m.partitions().containsKey(top.groupId()))
                continue;

            GridDhtPartitionMap locMap = top.localPartitionMap();

            addPartitionMap(m,
                dupData,
                true,
                top.groupId(),
                locMap,
                top.similarAffinityKey());

            if (sndCounters)
                m.partitionUpdateCounters(top.groupId(), top.updateCounters(true));
        }

        return m;
    }

    /**
     * @param m Message.
     * @param dupData Duplicated data map.
     * @param compress {@code True} if need check for duplicated partition state data.
     * @param cacheId Cache ID.
     * @param map Map to add.
     * @param affKey Cache affinity key.
     */
    private void addPartitionMap(GridDhtPartitionsSingleMessage m,
        Map<Object, T2<Integer, Map<Integer, GridDhtPartitionState>>> dupData,
        boolean compress,
        Integer cacheId,
        GridDhtPartitionMap map,
        Object affKey) {
        Integer dupDataCache = null;

        if (compress) {
            T2<Integer, Map<Integer, GridDhtPartitionState>> state0 = dupData.get(affKey);

            if (state0 != null && state0.get2().equals(map.map())) {
                dupDataCache = state0.get1();

                map = map.emptyCopy();
            }
            else
                dupData.put(affKey, new T2<>(cacheId, map.map()));
        }

        m.addLocalPartitionMap(cacheId, map, dupDataCache);
    }

    /**
     * @param nodeId Cause node ID.
     * @param topVer Topology version.
     * @param evt Event type.
     * @return Activity future ID.
     */
    private GridDhtPartitionExchangeId exchangeId(UUID nodeId, AffinityTopologyVersion topVer, int evt) {
        return new GridDhtPartitionExchangeId(nodeId, evt, topVer);
    }

    /**
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     * @param cache Discovery data cache.
     * @param exchActions Cache change actions.
     * @param affChangeMsg Affinity change message.
     * @return Exchange future.
     */
    private GridDhtPartitionsExchangeFuture exchangeFuture(GridDhtPartitionExchangeId exchId,
        @Nullable DiscoveryEvent discoEvt,
        @Nullable DiscoCache cache,
        @Nullable ExchangeActions exchActions,
        @Nullable CacheAffinityChangeMessage affChangeMsg) {
        GridDhtPartitionsExchangeFuture fut;

        GridDhtPartitionsExchangeFuture old = exchFuts.addx(
            fut = new GridDhtPartitionsExchangeFuture(cctx, busyLock, exchId, exchActions, affChangeMsg));

        if (old != null) {
            fut = old;

            if (exchActions != null)
                fut.exchangeActions(exchActions);

            if (affChangeMsg != null)
                fut.affinityChangeMessage(affChangeMsg);
        }

        if (discoEvt != null)
            fut.onEvent(exchId, discoEvt, cache);

        if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * @param exchFut Exchange.
     * @param err Error.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture exchFut, @Nullable Throwable err) {
        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        if (log.isDebugEnabled())
            log.debug("Exchange done [topVer=" + topVer + ", fut=" + exchFut + ", err=" + err + ']');

        IgniteProductVersion minVer = cctx.localNode().version();
        IgniteProductVersion maxVer = cctx.localNode().version();

        if (err == null) {
            if (!F.isEmpty(exchFut.discoveryEvent().topologyNodes())) {
                for (ClusterNode node : exchFut.discoveryEvent().topologyNodes()) {
                    IgniteProductVersion ver = node.version();

                    if (ver.compareTo(minVer) < 0)
                        minVer = ver;

                    if (ver.compareTo(maxVer) > 0)
                        maxVer = ver;
                }
            }
        }

        nodeVers.put(topVer, new IgnitePair<>(minVer, maxVer));

        AffinityTopologyVersion histVer = new AffinityTopologyVersion(topVer.topologyVersion() - 10, 0);

        for (AffinityTopologyVersion oldVer : nodeVers.headMap(histVer).keySet())
            nodeVers.remove(oldVer);

        if (err == null) {
            while (true) {
                AffinityTopologyVersion readyVer = readyTopVer.get();

                if (readyVer.compareTo(topVer) >= 0)
                    break;

                if (readyTopVer.compareAndSet(readyVer, topVer))
                    break;
            }

            for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
                if (entry.getKey().compareTo(topVer) <= 0) {
                    if (log.isDebugEnabled())
                        log.debug("Completing created topology ready future " +
                            "[ver=" + topVer + ", fut=" + entry.getValue() + ']');

                    entry.getValue().onDone(topVer);
                }
            }
        }
        else {
            for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
                if (entry.getKey().compareTo(topVer) <= 0) {
                    if (log.isDebugEnabled())
                        log.debug("Completing created topology ready future with error " +
                            "[ver=" + topVer + ", fut=" + entry.getValue() + ']');

                    entry.getValue().onDone(err);
                }
            }
        }

        ExchangeFutureSet exchFuts0 = exchFuts;

        if (exchFuts0 != null) {
            int skipped = 0;

            for (GridDhtPartitionsExchangeFuture fut : exchFuts0.values()) {
                if (exchFut.exchangeId().topologyVersion().compareTo(fut.exchangeId().topologyVersion()) < 0)
                    continue;

                skipped++;

                if (skipped > 10)
                    fut.cleanUp();
            }
        }
    }

    /**
     * @param fut Future.
     * @return {@code True} if added.
     */
    private boolean addFuture(GridDhtPartitionsExchangeFuture fut) {
        if (fut.onAdded()) {
            exchWorker.addExchangeFuture(fut);

            return true;
        }

        return false;
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void processFullPartitionUpdate(ClusterNode node, GridDhtPartitionsFullMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received full partition update [node=" + node.id() + ", msg=" + msg + ']');

                boolean updated = false;

                for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
                    Integer grpId = entry.getKey();

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    GridDhtPartitionTopology top = null;

                    if (grp == null)
                        top = clientTops.get(grpId);
                    else if (!grp.isLocal())
                        top = grp.topology();

                    if (top != null)
                        updated |= top.update(null, entry.getValue(), null);
                }

                if (!cctx.kernalContext().clientNode() && updated)
                    refreshPartitions();
            }
            else
                exchangeFuture(msg.exchangeId(), null, null, null, null).onReceive(node, msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionUpdate(final ClusterNode node, final GridDhtPartitionsSingleMessage msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received local partition update [nodeId=" + node.id() + ", parts=" +
                        msg + ']');

                boolean updated = false;

                for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
                    Integer grpId = entry.getKey();

                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    if (grp != null &&
                        grp.localStartVersion().compareTo(entry.getValue().topologyVersion()) > 0)
                        continue;

                    GridDhtPartitionTopology top = null;

                    if (grp == null)
                        top = clientTops.get(grpId);
                    else if (!grp.isLocal())
                        top = grp.topology();

                    if (top != null) {
                        updated |= top.update(null, entry.getValue());

                        cctx.affinity().checkRebalanceState(top, grpId);
                    }
                }

                if (updated)
                    scheduleResendPartitions();
            }
            else {
                if (msg.client()) {
                    final GridDhtPartitionsExchangeFuture exchFut = exchangeFuture(
                        msg.exchangeId(),
                        null,
                        null,
                        null,
                        null);

                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                            // Finished future should reply only to sender client node.
                            exchFut.onReceive(node, msg);
                        }
                    });
                }
                else
                    exchangeFuture(msg.exchangeId(), null, null, null, null).onReceive(node, msg);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionRequest(ClusterNode node, GridDhtPartitionsSingleRequest msg) {
        if (!enterBusy())
            return;

        try {
            sendLocalPartitions(node, msg.exchangeId());
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param exchFut Optional current exchange future.
     * @throws Exception If failed.
     */
    public void dumpDebugInfo(@Nullable GridDhtPartitionsExchangeFuture exchFut) throws Exception {
        AffinityTopologyVersion exchTopVer = exchFut != null ? exchFut.topologyVersion() : null;

        U.warn(diagnosticLog, "Ready affinity version: " + readyTopVer.get());

        U.warn(diagnosticLog, "Last exchange future: " + lastInitializedFut);

        exchWorker.dumpExchangeDebugInfo();

        if (!readyFuts.isEmpty()) {
            U.warn(diagnosticLog, "Pending affinity ready futures:");

            for (AffinityReadyFuture fut : readyFuts.values())
                U.warn(diagnosticLog, ">>> " + fut);
        }

        IgniteDiagnosticPrepareContext diagCtx = cctx.kernalContext().cluster().diagnosticEnabled() ?
            new IgniteDiagnosticPrepareContext(cctx.localNodeId()) : null;

        if (diagCtx != null && exchFut != null)
            exchFut.addDiagnosticRequest(diagCtx);

        ExchangeFutureSet exchFuts = this.exchFuts;

        if (exchFuts != null) {
            U.warn(diagnosticLog, "Last 10 exchange futures (total: " + exchFuts.size() + "):");

            int cnt = 0;

            for (GridDhtPartitionsExchangeFuture fut : exchFuts.values()) {
                U.warn(diagnosticLog, ">>> " + fut.shortInfo());

                if (++cnt == 10)
                    break;
            }
        }

        dumpPendingObjects(exchTopVer, diagCtx);

        for (CacheGroupContext grp : cctx.cache().cacheGroups())
            grp.preloader().dumpDebugInfo();

        cctx.affinity().dumpDebugInfo();

        cctx.io().dumpPendingMessages();

        if (IgniteSystemProperties.getBoolean(IGNITE_IO_DUMP_ON_TIMEOUT, false))
            cctx.gridIO().dumpStats();

        if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
            U.dumpThreads(diagnosticLog);

        if (diagCtx != null)
            diagCtx.send(cctx.kernalContext(), null);
    }

    /**
     * @param timeout Operation timeout.
     * @return {@code True} if found long running operations.
     */
    private boolean dumpLongRunningOperations0(long timeout) {
        long curTime = U.currentTimeMillis();

        boolean found = false;

        IgniteTxManager tm = cctx.tm();

        GridCacheMvccManager mvcc = cctx.mvcc();

        final IgniteDiagnosticPrepareContext diagCtx = cctx.kernalContext().cluster().diagnosticEnabled() ?
            new IgniteDiagnosticPrepareContext(cctx.localNodeId()) : null;

        if (tm != null) {
            for (IgniteInternalTx tx : tm.activeTransactions()) {
                if (curTime - tx.startTime() > timeout) {
                    found = true;

                    U.warn(diagnosticLog, "Found long running transaction [startTime=" + formatTime(tx.startTime()) +
                        ", curTime=" + formatTime(curTime) + ", tx=" + tx + ']');
                }
            }
        }

        if (mvcc != null) {
            for (GridCacheFuture<?> fut : mvcc.activeFutures()) {
                if (curTime - fut.startTime() > timeout) {
                    found = true;

                    U.warn(diagnosticLog, "Found long running cache future [startTime=" + formatTime(fut.startTime()) +
                        ", curTime=" + formatTime(curTime) + ", fut=" + fut + ']');

                    if (diagCtx != null && fut instanceof IgniteDiagnosticAware)
                        ((IgniteDiagnosticAware)fut).addDiagnosticRequest(diagCtx);
                }
            }

            for (GridCacheFuture<?> fut : mvcc.atomicFutures()) {
                if (curTime - fut.startTime() > timeout) {
                    found = true;

                    U.warn(diagnosticLog, "Found long running cache future [startTime=" + formatTime(fut.startTime()) +
                        ", curTime=" + formatTime(curTime) + ", fut=" + fut + ']');

                    if (diagCtx != null && fut instanceof IgniteDiagnosticAware)
                        ((IgniteDiagnosticAware)fut).addDiagnosticRequest(diagCtx);
                }
            }
        }

        if (diagCtx != null && !diagCtx.empty()) {
            try {
                cctx.kernalContext().closure().runLocal(new Runnable() {
                    @Override public void run() {
                        diagCtx.send(cctx.kernalContext(), null);
                    }
                }, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                U.error(diagnosticLog, "Failed to submit diagnostic closure: " + e, e);
            }
        }

        return found;
    }

    /**
     * @param timeout Operation timeout.
     */
    public void dumpLongRunningOperations(long timeout) {
        try {
            GridDhtPartitionsExchangeFuture lastFut = lastInitializedFut;

            // If exchange is in progress it will dump all hanging operations if any.
            if (lastFut != null && !lastFut.isDone())
                return;

            if (U.currentTimeMillis() < nextLongRunningOpsDumpTime)
                return;

            if (dumpLongRunningOperations0(timeout)) {
                nextLongRunningOpsDumpTime = U.currentTimeMillis() + nextDumpTimeout(longRunningOpsDumpStep++, timeout);

                if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false)) {
                    U.warn(diagnosticLog, "Found long running cache operations, dump threads.");

                    U.dumpThreads(diagnosticLog);
                }

                if (IgniteSystemProperties.getBoolean(IGNITE_IO_DUMP_ON_TIMEOUT, false)) {
                    U.warn(diagnosticLog, "Found long running cache operations, dump IO statistics.");

                    // Dump IO manager statistics.
                    cctx.gridIO().dumpStats();
                }
            }
            else {
                nextLongRunningOpsDumpTime = 0;
                longRunningOpsDumpStep = 0;
            }
        }
        catch (Exception e) {
            U.error(diagnosticLog, "Failed to dump debug information: " + e, e);
        }
    }

    /**
     * @param time Time.
     * @return Time string.
     */
    private String formatTime(long time) {
        return dateFormat.format(new Date(time));
    }

    /**
     * Check if provided task from exchange queue is exchange task.
     *
     * @param task Task.
     * @return {@code True} if this is exchange task.
     */
    private static boolean isExchangeTask(CachePartitionExchangeWorkerTask task) {
        return task instanceof GridDhtPartitionsExchangeFuture;
    }

    /**
     * @param exchTopVer Exchange topology version.
     * @param diagCtx Diagnostic request.
     */
    private void dumpPendingObjects(@Nullable AffinityTopologyVersion exchTopVer,
        @Nullable IgniteDiagnosticPrepareContext diagCtx) {
        IgniteTxManager tm = cctx.tm();

        if (tm != null) {
            U.warn(diagnosticLog, "Pending transactions:");

            for (IgniteInternalTx tx : tm.activeTransactions()) {
                if (exchTopVer != null) {
                    U.warn(diagnosticLog, ">>> [txVer=" + tx.topologyVersionSnapshot() +
                        ", exchWait=" + tm.needWaitTransaction(tx, exchTopVer) +
                        ", tx=" + tx + ']');
                }
                else
                    U.warn(diagnosticLog, ">>> [txVer=" + tx.topologyVersionSnapshot() + ", tx=" + tx + ']');
            }
        }

        GridCacheMvccManager mvcc = cctx.mvcc();

        if (mvcc != null) {
            U.warn(diagnosticLog, "Pending explicit locks:");

            for (GridCacheExplicitLockSpan lockSpan : mvcc.activeExplicitLocks())
                U.warn(diagnosticLog, ">>> " + lockSpan);

            U.warn(diagnosticLog, "Pending cache futures:");

            for (GridCacheFuture<?> fut : mvcc.activeFutures())
                dumpDiagnosticInfo(fut, diagCtx);

            U.warn(diagnosticLog, "Pending atomic cache futures:");

            for (GridCacheFuture<?> fut : mvcc.atomicFutures())
                dumpDiagnosticInfo(fut, diagCtx);

            U.warn(diagnosticLog, "Pending data streamer futures:");

            for (IgniteInternalFuture<?> fut : mvcc.dataStreamerFutures())
                dumpDiagnosticInfo(fut, diagCtx);

            if (tm != null) {
                U.warn(diagnosticLog, "Pending transaction deadlock detection futures:");

                for (IgniteInternalFuture<?> fut : tm.deadlockDetectionFutures())
                    dumpDiagnosticInfo(fut, diagCtx);
            }
        }

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            GridCachePreloader preloader = grp.preloader();

            if (preloader != null)
                preloader.dumpDebugInfo();

            GridAffinityAssignmentCache aff = grp.affinity();

            if (aff != null)
                aff.dumpDebugInfo();
        }
    }

    /**
     * Logs the future and add diagnostic info closure.
     *
     * @param fut Future.
     * @param ctx Diagnostic prepare context.
     */
    private void dumpDiagnosticInfo(IgniteInternalFuture<?> fut,
        @Nullable IgniteDiagnosticPrepareContext ctx) {
        U.warn(diagnosticLog, ">>> " + fut);

        if (ctx != null && fut instanceof IgniteDiagnosticAware)
            ((IgniteDiagnosticAware)fut).addDiagnosticRequest(ctx);
    }

    /**
     * @param deque Deque to poll from.
     * @param time Time to wait.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> deque, long time, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return deque.poll(time, MILLISECONDS);
    }

    /**
     * Exchange future thread. All exchanges happen only by one thread and next
     * exchange will not start until previous one completes.
     */
    private class ExchangeWorker extends GridWorker {
        /** Future queue. */
        private final LinkedBlockingDeque<CachePartitionExchangeWorkerTask> futQ =
            new LinkedBlockingDeque<>();

        /** Busy flag used as performance optimization to stop current preloading. */
        private volatile boolean busy;

        /**
         * Constructor.
         */
        private ExchangeWorker() {
            super(cctx.igniteInstanceName(), "partition-exchanger", GridCachePartitionExchangeManager.this.log);
        }

        /**
         * Add first exchange future.
         *
         * @param exchFut Exchange future.
         */
        void addFirstExchangeFuture(GridDhtPartitionsExchangeFuture exchFut) {
            futQ.addFirst(exchFut);
        }

        /**
         * @param exchFut Exchange future.
         */
        void addExchangeFuture(GridDhtPartitionsExchangeFuture exchFut) {
            assert exchFut != null;

            if (!exchFut.dummy() || (!hasPendingExchange() && !busy))
                futQ.offer(exchFut);

            if (log.isDebugEnabled())
                log.debug("Added exchange future to exchange worker: " + exchFut);
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            synchronized (interruptLock) {
                super.cancel();
            }
        }

        /**
         * Add custom exchange task.
         *
         * @param task Task.
         */
        void addCustomTask(CachePartitionExchangeWorkerTask task) {
            assert task != null;

            assert !isExchangeTask(task);

            futQ.offer(task);
        }

        /**
         * Process custom exchange task.
         *
         * @param task Task.
         */
        void processCustomTask(CachePartitionExchangeWorkerTask task) {
            assert !isExchangeTask(task);

            try {
                cctx.cache().processCustomExchangeTask(task);
            }
            catch (Exception e) {
                U.error(log, "Failed to process custom exchange task: " + task, e);
            }
        }

        /**
         * @return Whether pending exchange future exists.
         */
        boolean hasPendingExchange() {
            if (!futQ.isEmpty()) {
                for (CachePartitionExchangeWorkerTask task : futQ) {
                    if (isExchangeTask(task))
                        return true;
                }
            }

            return false;
        }

        /**
         * Dump debug info.
         */
        void dumpExchangeDebugInfo() {
            U.warn(log, "Pending exchange futures:");

            for (CachePartitionExchangeWorkerTask task: futQ) {
                if (isExchangeTask(task))
                    U.warn(log, ">>> " + ((GridDhtPartitionsExchangeFuture)task).shortInfo());
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            long timeout = cctx.gridConfig().getNetworkTimeout();

            int cnt = 0;

            while (!isCancelled()) {
                cnt++;

                CachePartitionExchangeWorkerTask task = null;

                try {
                    boolean preloadFinished = true;

                    for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                        if (grp.isLocal())
                            continue;

                        preloadFinished &= grp.preloader() != null && grp.preloader().syncFuture().isDone();

                        if (!preloadFinished)
                            break;
                    }

                    // If not first preloading and no more topology events present.
                    if (!cctx.kernalContext().clientNode() && !hasPendingExchange() && preloadFinished)
                        timeout = cctx.gridConfig().getNetworkTimeout();

                    // After workers line up and before preloading starts we initialize all futures.
                    if (log.isDebugEnabled()) {
                        Collection<IgniteInternalFuture> unfinished = new HashSet<>();

                        for (GridDhtPartitionsExchangeFuture fut : exchFuts.values()) {
                            if (!fut.isDone())
                                unfinished.add(fut);
                        }

                        log.debug("Before waiting for exchange futures [futs" + unfinished + ", worker=" + this + ']');
                    }

                    // Take next exchange future.
                    if (isCancelled())
                        Thread.currentThread().interrupt();

                    task = futQ.poll(timeout, MILLISECONDS);

                    if (task == null)
                        continue; // Main while loop.

                    if (!isExchangeTask(task)) {
                        processCustomTask(task);

                        continue;
                    }

                    assert task instanceof GridDhtPartitionsExchangeFuture;

                    GridDhtPartitionsExchangeFuture exchFut = (GridDhtPartitionsExchangeFuture)task;

                    busy = true;

                    Map<Integer, GridDhtPreloaderAssignments> assignsMap = null;

                    boolean dummyReassign = exchFut.dummyReassign();
                    boolean forcePreload = exchFut.forcePreload();

                    try {
                        if (isCancelled())
                            break;

                        if (!exchFut.dummy() && !exchFut.forcePreload()) {
                            lastInitializedFut = exchFut;

                            exchFut.init();

                            int dumpCnt = 0;

                            final long futTimeout = 2 * cctx.gridConfig().getNetworkTimeout();

                            long nextDumpTime = 0;

                            while (true) {
                                try {
                                    exchFut.get(futTimeout, TimeUnit.MILLISECONDS);

                                    break;
                                }
                                catch (IgniteFutureTimeoutCheckedException ignored) {
                                    U.warn(diagnosticLog, "Failed to wait for partition map exchange [" +
                                        "topVer=" + exchFut.topologyVersion() +
                                        ", node=" + cctx.localNodeId() + "]. " +
                                        "Dumping pending objects that might be the cause: ");

                                    if (nextDumpTime <= U.currentTimeMillis()) {
                                        try {
                                            dumpDebugInfo(exchFut);
                                        }
                                        catch (Exception e) {
                                            U.error(diagnosticLog, "Failed to dump debug information: " + e, e);
                                        }

                                        nextDumpTime = U.currentTimeMillis() + nextDumpTimeout(dumpCnt++, futTimeout);
                                    }
                                }
                                catch (Exception e) {
                                    if (exchFut.reconnectOnError(e))
                                        throw new IgniteNeedReconnectException(cctx.localNode(), e);

                                    throw e;
                                }
                            }


                            if (log.isDebugEnabled())
                                log.debug("After waiting for exchange future [exchFut=" + exchFut + ", worker=" +
                                    this + ']');

                            if (exchFut.exchangeId().nodeId().equals(cctx.localNodeId()))
                                lastRefresh.compareAndSet(-1, U.currentTimeMillis());

                            // Just pick first worker to do this, so we don't
                            // invoke topology callback more than once for the
                            // same event.

                            boolean changed = false;

                            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                                if (grp.isLocal())
                                    continue;

                                changed |= grp.topology().afterExchange(exchFut);
                            }

                            if (!cctx.kernalContext().clientNode() && changed && !hasPendingExchange())
                                refreshPartitions();
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Got dummy exchange (will reassign)");

                            if (!dummyReassign) {
                                timeout = 0; // Force refresh.

                                continue;
                            }
                        }

                        if (!exchFut.skipPreload() && cctx.kernalContext().state().active()) {
                            assignsMap = new HashMap<>();

                            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                                long delay = grp.config().getRebalanceDelay();

                                GridDhtPreloaderAssignments assigns = null;

                                // Don't delay for dummy reassigns to avoid infinite recursion.
                                if (delay == 0 || forcePreload)
                                    assigns = grp.preloader().assign(exchFut);

                                assignsMap.put(grp.groupId(), assigns);
                            }
                        }
                    }
                    finally {
                        // Must flip busy flag before assignments are given to demand workers.
                        busy = false;
                    }

                    if (assignsMap != null) {
                        int size = assignsMap.size();

                        NavigableMap<Integer, List<Integer>> orderMap = new TreeMap<>();

                        for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignsMap.entrySet()) {
                            int grpId = e.getKey();

                            CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                            int order = grp.config().getRebalanceOrder();

                            if (orderMap.get(order) == null)
                                orderMap.put(order, new ArrayList<Integer>(size));

                            orderMap.get(order).add(grpId);
                        }

                        Runnable r = null;

                        List<String> rebList = new LinkedList<>();

                        boolean assignsCancelled = false;

                        for (Integer order : orderMap.descendingKeySet()) {
                            for (Integer grpId : orderMap.get(order)) {
                                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                                GridDhtPreloaderAssignments assigns = assignsMap.get(grpId);

                                if (assigns != null)
                                    assignsCancelled |= assigns.cancelled();

                                // Cancels previous rebalance future (in case it's not done yet).
                                // Sends previous rebalance stopped event (if necessary).
                                // Creates new rebalance future.
                                // Sends current rebalance started event (if necessary).
                                // Finishes cache sync future (on empty assignments).
                                Runnable cur = grp.preloader().addAssignments(assigns,
                                    forcePreload,
                                    cnt,
                                    r,
                                    exchFut.forcedRebalanceFuture());

                                if (cur != null) {
                                    rebList.add(grp.cacheOrGroupName());

                                    r = cur;
                                }
                            }
                        }

                        if (exchFut.forcedRebalanceFuture() != null)
                            exchFut.forcedRebalanceFuture().markInitialized();

                        if (assignsCancelled) { // Pending exchange.
                            U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                                "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                        }
                        else if (r != null) {
                            Collections.reverse(rebList);

                            U.log(log, "Rebalancing scheduled [order=" + rebList + "]");

                            if (!hasPendingExchange()) {
                                U.log(log, "Rebalancing started " +
                                    "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                    ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');

                                r.run(); // Starts rebalancing routine.
                            }
                            else
                                U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                                    "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                    ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                        }
                        else
                            U.log(log, "Skipping rebalancing (nothing scheduled) " +
                                "[top=" + exchFut.topologyVersion() + ", evt=" + exchFut.discoveryEvent().name() +
                                ", node=" + exchFut.discoveryEvent().eventNode().id() + ']');
                    }
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e;
                }
                catch (IgniteClientDisconnectedCheckedException | IgniteNeedReconnectException e) {
                    assert cctx.discovery().reconnectSupported();

                    U.warn(log,"Local node failed to complete partition map exchange due to " +
                        "network issues, will try to reconnect to cluster", e);

                    cctx.discovery().reconnect();

                    return;
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to wait for completion of partition map exchange " +
                        "(preloading will not start): " + task, e);
                }
            }
        }
    }

    /**
     * Partition resend timeout object.
     */
    private class ResendTimeoutObject implements GridTimeoutObject {
        /** Timeout ID. */
        private final IgniteUuid timeoutId = IgniteUuid.randomUuid();

        /** Timeout start time. */
        private final long createTime = U.currentTimeMillis();

        /** Started flag. */
        private AtomicBoolean started = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return createTime + partResendTimeout;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                @Override public void run() {
                    if (!busyLock.readLock().tryLock())
                        return;

                    try {
                        if (started.compareAndSet(false, true))
                            refreshPartitions();
                    }
                    finally {
                        busyLock.readLock().unlock();

                        cctx.time().removeTimeoutObject(ResendTimeoutObject.this);

                        pendingResend.compareAndSet(ResendTimeoutObject.this, null);
                    }
                }
            });
        }

        /**
         * @return {@code True} if timeout object started to run.
         */
        public boolean started() {
            return started.get();
        }
    }

    /**
     *
     */
    private static class ExchangeFutureSet extends GridListSet<GridDhtPartitionsExchangeFuture> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates ordered, not strict list set.
         */
        private ExchangeFutureSet() {
            super(new Comparator<GridDhtPartitionsExchangeFuture>() {
                @Override public int compare(
                    GridDhtPartitionsExchangeFuture f1,
                    GridDhtPartitionsExchangeFuture f2
                ) {
                    AffinityTopologyVersion t1 = f1.exchangeId().topologyVersion();
                    AffinityTopologyVersion t2 = f2.exchangeId().topologyVersion();

                    assert t1.topologyVersion() > 0;
                    assert t2.topologyVersion() > 0;

                    // Reverse order.
                    return t2.compareTo(t1);
                }
            }, /*not strict*/false);
        }

        /**
         * @param fut Future to add.
         * @return {@code True} if added.
         */
        @Override public synchronized GridDhtPartitionsExchangeFuture addx(
            GridDhtPartitionsExchangeFuture fut) {
            GridDhtPartitionsExchangeFuture cur = super.addx(fut);

            while (size() > EXCHANGE_HISTORY_SIZE) {
                GridDhtPartitionsExchangeFuture last = last();

                if (!last.isDone())
                    break;

                removeLast();
            }

            // Return the value in the set.
            return cur == null ? fut : cur;
        }

        /** {@inheritDoc} */
        @Nullable @Override public synchronized GridDhtPartitionsExchangeFuture removex(
            GridDhtPartitionsExchangeFuture val) {

            return super.removex(val);
        }

        /**
         * @return Values.
         */
        @Override public synchronized List<GridDhtPartitionsExchangeFuture> values() {
            return super.values();
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return S.toString(ExchangeFutureSet.class, this, super.toString());
        }
    }

    /**
     *
     */
    private abstract class MessageHandler<M> implements IgniteBiInClosure<UUID, M> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(UUID nodeId, M msg) {
            ClusterNode node = cctx.node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Received message from failed node [node=" + nodeId + ", msg=" + msg + ']');

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Received message from node [node=" + nodeId + ", msg=" + msg + ']');

            onMessage(node, msg);
        }

        /**
         * @param node Node.
         * @param msg Message.
         */
        protected abstract void onMessage(ClusterNode node, M msg);
    }

    /**
     * Affinity ready future.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        @GridToStringInclude
        private AffinityTopologyVersion topVer;

        /**
         * @param topVer Topology version.
         */
        private AffinityReadyFuture(AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(AffinityTopologyVersion res, @Nullable Throwable err) {
            assert res != null || err != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(topVer, this);

            return done;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityReadyFuture.class, this, super.toString());
        }
    }
}
