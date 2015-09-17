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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAssignmentFetchFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;
import static org.apache.ignite.internal.util.GridConcurrentFactory.newMap;

/**
 * DHT cache preloader.
 */
public class GridDhtPreloader extends GridCachePreloaderAdapter {
    /** Default preload resend timeout. */
    public static final long DFLT_PRELOAD_RESEND_TIMEOUT = 1500;

    /** */
    private GridDhtPartitionTopology top;

    /** Topology version. */
    private final GridAtomicLong topVer = new GridAtomicLong();

    /** Force key futures. */
    private final ConcurrentMap<IgniteUuid, GridDhtForceKeysFuture<?, ?>> forceKeyFuts = newMap();

    /** Partition suppliers. */
    private GridDhtPartitionSupplyPool supplyPool;

    /** Partition demanders. */
    private GridDhtPartitionDemandPool demandPool;

    /** Start future. */
    private GridFutureAdapter<Object> startFut;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Pending affinity assignment futures. */
    private ConcurrentMap<AffinityTopologyVersion, GridDhtAssignmentFetchFuture> pendingAssignmentFetchFuts =
        new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            if (!enterBusy())
                return;

            DiscoveryEvent e = (DiscoveryEvent)evt;

            try {
                ClusterNode loc = cctx.localNode();

                assert e.type() == EVT_NODE_JOINED || e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

                final ClusterNode n = e.eventNode();

                assert !loc.id().equals(n.id());

                for (GridDhtForceKeysFuture<?, ?> f : forceKeyFuts.values())
                    f.onDiscoveryEvent(e);

                assert e.type() != EVT_NODE_JOINED || n.order() > loc.order() : "Node joined with smaller-than-local " +
                    "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ']';

                boolean set = topVer.setIfGreater(e.topologyVersion());

                assert set : "Have you configured TcpDiscoverySpi for your in-memory data grid? [newVer=" +
                    e.topologyVersion() + ", curVer=" + topVer.get() + ", evt=" + e + ']';

                if (e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED) {
                    for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
                        fut.onNodeLeft(e.eventNode().id());
                }
            }
            finally {
                leaveBusy();
            }
        }
    };

    /**
     * @param cctx Cache context.
     */
    public GridDhtPreloader(GridCacheContext<?, ?> cctx) {
        super(cctx);

        top = cctx.dht().topology();

        startFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (log.isDebugEnabled())
            log.debug("Starting DHT rebalancer...");

        cctx.io().addHandler(cctx.cacheId(), GridDhtForceKeysRequest.class,
            new MessageHandler<GridDhtForceKeysRequest>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysRequest msg) {
                    processForceKeysRequest(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtForceKeysResponse.class,
            new MessageHandler<GridDhtForceKeysResponse>() {
                @Override public void onMessage(ClusterNode node, GridDhtForceKeysResponse msg) {
                    processForceKeyResponse(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtAffinityAssignmentRequest.class,
            new MessageHandler<GridDhtAffinityAssignmentRequest>() {
                @Override protected void onMessage(ClusterNode node, GridDhtAffinityAssignmentRequest msg) {
                    processAffinityAssignmentRequest(node, msg);
                }
            });

        cctx.io().addHandler(cctx.cacheId(), GridDhtAffinityAssignmentResponse.class,
            new MessageHandler<GridDhtAffinityAssignmentResponse>() {
                @Override protected void onMessage(ClusterNode node, GridDhtAffinityAssignmentResponse msg) {
                    processAffinityAssignmentResponse(node, msg);
                }
            });

        supplyPool = new GridDhtPartitionSupplyPool(cctx, busyLock);
        demandPool = new GridDhtPartitionDemandPool(cctx, busyLock);

        cctx.events().addListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("DHT rebalancer onKernalStart callback.");

        ClusterNode loc = cctx.localNode();

        long startTime = loc.metrics().getStartTime();

        assert startTime > 0;

        final long startTopVer = loc.order();

        topVer.setIfGreater(startTopVer);

        supplyPool.start();
        demandPool.start();
    }

    /** {@inheritDoc} */
    @Override public void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
        super.preloadPredicate(preloadPred);

        assert supplyPool != null && demandPool != null : "preloadPredicate may be called only after start()";

        supplyPool.preloadPredicate(preloadPred);
        demandPool.preloadPredicate(preloadPred);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("DHT rebalancer onKernalStop callback.");

        cctx.events().removeListener(discoLsnr);

        // Acquire write busy lock.
        busyLock.writeLock().lock();

        if (supplyPool != null)
            supplyPool.stop();

        if (demandPool != null)
            demandPool.stop();

        top = null;
    }

    /** {@inheritDoc} */
    @Override public void onInitialExchangeComplete(@Nullable Throwable err) {
        if (err == null) {
            startFut.onDone();

            final long start = U.currentTimeMillis();

            final CacheConfiguration cfg = cctx.config();

            if (cfg.getRebalanceDelay() >= 0 && !cctx.kernalContext().clientNode()) {
                U.log(log, "Starting rebalancing in " + cfg.getRebalanceMode() + " mode: " + cctx.name());

                demandPool.syncFuture().listen(new CI1<Object>() {
                    @Override public void apply(Object t) {
                        U.log(log, "Completed rebalancing in " + cfg.getRebalanceMode() + " mode " +
                            "[cache=" + cctx.name() + ", time=" + (U.currentTimeMillis() - start) + " ms]");
                    }
                });
            }
        }
        else
            startFut.onDone(err);
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        startFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public void onExchangeFutureAdded() {
        demandPool.onExchangeFutureAdded();
    }

    /** {@inheritDoc} */
    @Override public void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut) {
        demandPool.updateLastExchangeFuture(lastFut);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut) {
        return demandPool.assign(exchFut);
    }

    /** {@inheritDoc} */
    @Override public void addAssignments(GridDhtPreloaderAssignments assignments, boolean forcePreload) {
        demandPool.addAssignments(assignments, forcePreload);
    }

    /**
     * @return Start future.
     */
    @Override public IgniteInternalFuture<Object> startFuture() {
        return startFut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> syncFuture() {
        return cctx.kernalContext().clientNode() ? startFut : demandPool.syncFuture();
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(AffinityTopologyVersion topVer, GridDhtAssignmentFetchFuture fut) {
        GridDhtAssignmentFetchFuture old = pendingAssignmentFetchFuts.putIfAbsent(topVer, fut);

        assert old == null : "More than one thread is trying to fetch partition assignments: " + topVer;
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(AffinityTopologyVersion topVer, GridDhtAssignmentFetchFuture fut) {
        boolean rmv = pendingAssignmentFetchFuts.remove(topVer, fut);

        assert rmv : "Failed to remove assignment fetch future: " + topVer;
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter busy state on node (exchanger is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest(final ClusterNode node, final GridDhtForceKeysRequest msg) {
        IgniteInternalFuture<?> fut = cctx.mvcc().finishKeys(msg.keys(), msg.topologyVersion());

        if (fut.isDone())
            processForceKeysRequest0(node, msg);
        else
            fut.listen(new CI1<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> t) {
                    processForceKeysRequest0(node, msg);
                }
            });
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest0(ClusterNode node, GridDhtForceKeysRequest msg) {
        if (!enterBusy())
            return;

        try {
            ClusterNode loc = cctx.localNode();

            GridDhtForceKeysResponse res = new GridDhtForceKeysResponse(
                cctx.cacheId(),
                msg.futureId(),
                msg.miniId());

            for (KeyCacheObject k : msg.keys()) {
                int p = cctx.affinity().partition(k);

                GridDhtLocalPartition locPart = top.localPartition(p, AffinityTopologyVersion.NONE, false);

                // If this node is no longer an owner.
                if (locPart == null && !top.owners(p).contains(loc))
                    res.addMissed(k);

                GridCacheEntryEx entry;

                if (cctx.isSwapOrOffheapEnabled()) {
                    while (true) {
                        try {
                            entry = cctx.dht().entryEx(k);

                            entry.unswap();

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry: " + k);
                        }
                    }
                }
                else
                    entry = cctx.dht().peekEx(k);

                // If entry is null, then local partition may have left
                // after the message was received. In that case, we are
                // confident that primary node knows of any changes to the key.
                if (entry != null) {
                    GridCacheEntryInfo info = entry.info();

                    if (info != null && !info.isNew())
                        res.addInfo(info);

                    if (cctx.isSwapOrOffheapEnabled())
                        cctx.evicts().touch(entry, msg.topologyVersion());
                }
                else if (log.isDebugEnabled())
                    log.debug("Key is not present in DHT cache: " + k);
            }

            if (log.isDebugEnabled())
                log.debug("Sending force key response [node=" + node.id() + ", res=" + res + ']');

            cctx.io().send(node, res, cctx.ioPolicy());
        }
        catch (ClusterTopologyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received force key request form failed node (will ignore) [nodeId=" + node.id() +
                    ", req=" + msg + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to reply to force key request [nodeId=" + node.id() + ", req=" + msg + ']', e);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void processForceKeyResponse(ClusterNode node, GridDhtForceKeysResponse msg) {
        if (!enterBusy())
            return;

        try {
            GridDhtForceKeysFuture<?, ?> f = forceKeyFuts.get(msg.futureId());

            if (f != null)
                f.onResult(node.id(), msg);
            else if (log.isDebugEnabled())
                log.debug("Receive force key response for unknown future (is it duplicate?) [nodeId=" + node.id() +
                    ", res=" + msg + ']');
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node.
     * @param req Request.
     */
    private void processAffinityAssignmentRequest(final ClusterNode node,
        final GridDhtAffinityAssignmentRequest req) {
        final AffinityTopologyVersion topVer = req.topologyVersion();

        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment request [node=" + node + ", req=" + req + ']');

        cctx.affinity().affinityReadyFuture(req.topologyVersion()).listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
            @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                if (log.isDebugEnabled())
                    log.debug("Affinity is ready for topology version, will send response [topVer=" + topVer +
                        ", node=" + node + ']');

                List<List<ClusterNode>> assignment = cctx.affinity().assignments(topVer);

                try {
                    cctx.io().send(node,
                        new GridDhtAffinityAssignmentResponse(cctx.cacheId(), topVer, assignment), AFFINITY_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to send affinity assignment response to remote node [node=" + node + ']', e);
                }
            }
        });
    }

    /**
     * @param node Node.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(ClusterNode node, GridDhtAffinityAssignmentResponse res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + node + ", res=" + res + ']');

        for (GridDhtAssignmentFetchFuture fut : pendingAssignmentFetchFuts.values())
            fut.onResponse(node, res);
    }

    /**
     * Resends partitions on partition evict within configured timeout.
     *
     * @param part Evicted partition.
     * @param updateSeq Update sequence.
     */
    public void onPartitionEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        if (!enterBusy())
            return;

        try {
            top.onEvicted(part, updateSeq);

            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_UNLOADED))
                cctx.events().addUnloadEvent(part.id());

            if (updateSeq)
                cctx.shared().exchange().scheduleResendPartitions();
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param keys Keys to request.
     * @return Future for request.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public GridDhtFuture<Object> request(Collection<KeyCacheObject> keys, AffinityTopologyVersion topVer) {
        final GridDhtForceKeysFuture<?, ?> fut = new GridDhtForceKeysFuture<>(cctx, topVer, keys, this);

        IgniteInternalFuture<?> topReadyFut = cctx.affinity().affinityReadyFuturex(topVer);

        if (startFut.isDone() && topReadyFut == null)
            fut.init();
        else {
            if (topReadyFut == null)
                startFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        cctx.kernalContext().closure().runLocalSafe(
                            new GridPlainRunnable() {
                                @Override public void run() {
                                    fut.init();
                                }
                            });
                    }
                });
            else {
                GridCompoundFuture<Object, Object> compound = new GridCompoundFuture<>();

                compound.add((IgniteInternalFuture<Object>)startFut);
                compound.add((IgniteInternalFuture<Object>)topReadyFut);

                compound.markInitialized();

                compound.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        fut.init();
                    }
                });
            }
        }

        return (GridDhtFuture)fut;
    }

    /** {@inheritDoc} */
    @Override public void forcePreload() {
        demandPool.forcePreload();
    }

    /** {@inheritDoc} */
    @Override public void unwindUndeploys() {
        demandPool.unwindUndeploys();
    }

    /**
     * Adds future to future map.
     *
     * @param fut Future to add.
     */
    void addFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.put(fut.futureId(), fut);
    }

    /**
     * Removes future from future map.
     *
     * @param fut Future to remove.
     */
    void remoteFuture(GridDhtForceKeysFuture<?, ?> fut) {
        forceKeyFuts.remove(fut.futureId(), fut);
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

            onMessage(node , msg);
        }

        /**
         * @param node Node.
         * @param msg Message.
         */
        protected abstract void onMessage(ClusterNode node, M msg);
    }
}