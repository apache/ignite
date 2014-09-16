/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.util.GridConcurrentFactory.*;

/**
 * DHT cache preloader.
 */
public class GridDhtPreloader<K, V> extends GridCachePreloaderAdapter<K, V> {
    /** Default preload resend timeout. */
    public static final long DFLT_PRELOAD_RESEND_TIMEOUT = 1500;

    /** Exchange history size. */
    private static final int EXCHANGE_HISTORY_SIZE = 1000;

    /** */
    private GridDhtPartitionTopology<K, V> top;

    /**
     * Partition map futures.
     * This set also contains already completed exchange futures to address race conditions when coordinator
     * leaves grid and new coordinator sends full partition message to a node which has not yet received
     * discovery event. In case if remote node will retry partition exchange, completed future will indicate
     * that full partition map should be sent to requesting node right away.
     */
    private ExchangeFutureSet exchFuts = new ExchangeFutureSet();

    /** Topology version. */
    private final GridAtomicLong topVer = new GridAtomicLong();

    /** Force key futures. */
    private final ConcurrentMap<GridUuid, GridDhtForceKeysFuture<K, V>> forceKeyFuts = newMap();

    /** Partition suppliers. */
    private GridDhtPartitionSupplyPool<K, V> supplyPool;

    /** Partition demanders. */
    private GridDhtPartitionDemandPool<K, V> demandPool;

    /** Start future. */
    private final GridFutureAdapter<?> startFut;

    /** Latch which completes after local exchange future is created. */
    private final GridFutureAdapter<?> locExchFut;

    /** Pending futures. */
    private final Queue<GridDhtPartitionsExchangeFuture<K, V>> pendingExchangeFuts = new ConcurrentLinkedQueue<>();

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Pending affinity assignment futures. */
    private ConcurrentMap<Long, GridDhtAssignmentFetchFuture<K, V>> pendingAssignmentFetchFuts =
        new ConcurrentHashMap8<>();

    /** Discovery listener. */
    private final GridLocalEventListener discoLsnr = new GridLocalEventListener() {
        @Override public void onEvent(GridEvent evt) {
            if (!enterBusy())
                return;

            GridDiscoveryEvent e = (GridDiscoveryEvent)evt;

            try {
                GridNode loc = cctx.localNode();

                assert e.type() == EVT_NODE_JOINED || e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED;

                final GridNode n = e.eventNode();

                assert !loc.id().equals(n.id());

                for (GridDhtForceKeysFuture<K, V> f : forceKeyFuts.values())
                    f.onDiscoveryEvent(e);

                if (e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED) {
                    assert cctx.discovery().node(n.id()) == null;

                    for (GridDhtPartitionsExchangeFuture<K, V> f : exchFuts.values())
                        f.onNodeLeft(n.id());
                }

                assert e.type() != EVT_NODE_JOINED || n.order() > loc.order() : "Node joined with smaller-than-local " +
                    "order [newOrder=" + n.order() + ", locOrder=" + loc.order() + ']';

                boolean set = topVer.setIfGreater(e.topologyVersion());

                assert set : "Have you configured GridTcpDiscoverySpi for your in-memory data grid?";

                GridDhtPartitionExchangeId exchId = exchangeId(n.id(), e.topologyVersion(), e.type());

                GridDhtPartitionsExchangeFuture<K, V> exchFut = exchangeFuture(exchId, e);

                // Start exchange process.
                pendingExchangeFuts.add(exchFut);

                // Event callback - without this callback future will never complete.
                exchFut.onEvent(exchId, e);

                if (log.isDebugEnabled())
                    log.debug("Discovery event (will start exchange): " + exchId);

                locExchFut.listenAsync(new CI1<GridFuture<?>>() {
                    @Override public void apply(GridFuture<?> t) {
                        if (!enterBusy())
                            return;

                        try {
                            // Unwind in the order of discovery events.
                            for (GridDhtPartitionsExchangeFuture<K, V> f = pendingExchangeFuts.poll(); f != null;
                                f = pendingExchangeFuts.poll()) {
                                demandPool.onDiscoveryEvent(n.id(), f);
                            }
                        }
                        finally {
                            leaveBusy();
                        }
                    }
                });

                if (e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED) {
                    for (GridDhtAssignmentFetchFuture<K, V> fut : pendingAssignmentFetchFuts.values())
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
    public GridDhtPreloader(GridCacheContext<K, V> cctx) {
        super(cctx);

        top = cctx.dht().topology();

        locExchFut = new GridFutureAdapter<Object>(cctx.kernalContext(), true);
        startFut = new GridFutureAdapter<Object>(cctx.kernalContext());
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (log.isDebugEnabled())
            log.debug("Starting DHT preloader...");

        cctx.io().addHandler(GridDhtPartitionsSingleMessage.class,
            new MessageHandler<GridDhtPartitionsSingleMessage<K, V>>() {
                @Override public void onMessage(GridNode node, GridDhtPartitionsSingleMessage<K, V> msg) {
                    processSinglePartitionUpdate(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtPartitionsFullMessage.class,
            new MessageHandler<GridDhtPartitionsFullMessage<K, V>>() {
                @Override public void onMessage(GridNode node, GridDhtPartitionsFullMessage<K, V> msg) {
                    processFullPartitionUpdate(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtPartitionsSingleRequest.class,
            new MessageHandler<GridDhtPartitionsSingleRequest<K, V>>() {
                @Override public void onMessage(GridNode node, GridDhtPartitionsSingleRequest<K, V> msg) {
                    processSinglePartitionRequest(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtForceKeysRequest.class,
            new MessageHandler<GridDhtForceKeysRequest<K, V>>() {
                @Override public void onMessage(GridNode node, GridDhtForceKeysRequest<K, V> msg) {
                    processForceKeysRequest(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtForceKeysResponse.class,
            new MessageHandler<GridDhtForceKeysResponse<K, V>>() {
                @Override public void onMessage(GridNode node, GridDhtForceKeysResponse<K, V> msg) {
                    processForceKeyResponse(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtAffinityAssignmentRequest.class,
            new MessageHandler<GridDhtAffinityAssignmentRequest<K, V>>() {
                @Override protected void onMessage(GridNode node, GridDhtAffinityAssignmentRequest<K, V> msg) {
                    processAffinityAssignmentRequest(node, msg);
                }
            });

        cctx.io().addHandler(GridDhtAffinityAssignmentResponse.class,
            new MessageHandler<GridDhtAffinityAssignmentResponse<K, V>>() {
                @Override protected void onMessage(GridNode node, GridDhtAffinityAssignmentResponse<K, V> msg) {
                    processAffinityAssignmentResponse(node, msg);
                }
            });

        supplyPool = new GridDhtPartitionSupplyPool<>(cctx, busyLock);
        demandPool = new GridDhtPartitionDemandPool<>(cctx, busyLock);

        cctx.events().addListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        if (log.isDebugEnabled())
            log.debug("DHT preloader onKernalStart callback.");

        GridNode loc = cctx.localNode();

        long startTime = loc.metrics().getStartTime();

        assert startTime > 0;

        final long startTopVer = loc.order();

        topVer.setIfGreater(startTopVer);

        GridDhtPartitionExchangeId exchId = exchangeId(loc.id(), startTopVer, EVT_NODE_JOINED);

        // Generate dummy discovery event for local node joining.
        GridDiscoveryEvent discoEvt = cctx.discovery().localJoinEvent();

        assert discoEvt != null;

        assert discoEvt.topologyVersion() == startTopVer;

        GridDhtPartitionsExchangeFuture<K, V> fut = exchangeFuture(exchId, discoEvt);

        supplyPool.start();
        demandPool.start(fut);

        // Allow discovery events to get processed.
        locExchFut.onDone();

        if (log.isDebugEnabled())
            log.debug("Beginning to wait on local exchange future: " + fut);

        try {
            boolean first = true;

            while (true) {
                try {
                    fut.get(cctx.preloadExchangeTimeout());

                    break;
                }
                catch (GridFutureTimeoutException ignored) {
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
            }

            startFut.onDone();
        }
        catch (GridFutureTimeoutException e) {
            GridException err = new GridException("Timed out waiting for exchange future: " + fut, e);

            startFut.onDone(err);

            throw err;
        }

        if (log.isDebugEnabled())
            log.debug("Finished waiting on local exchange: " + fut.exchangeId());

        final long start = U.currentTimeMillis();

        if (cctx.config().getPreloadPartitionedDelay() >= 0) {
            U.log(log, "Starting preloading in " + cctx.config().getPreloadMode() + " mode: " + cctx.name());

            demandPool.syncFuture().listenAsync(new CI1<Object>() {
                @Override public void apply(Object t) {
                    U.log(log, "Completed preloading in " + cctx.config().getPreloadMode() + " mode " +
                        "[cache=" + cctx.name() + ", time=" + (U.currentTimeMillis() - start) + " ms]");
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        super.preloadPredicate(preloadPred);

        assert supplyPool != null && demandPool != null : "preloadPredicate may be called only after start()";

        supplyPool.preloadPredicate(preloadPred);
        demandPool.preloadPredicate(preloadPred);
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("DHT preloader onKernalStop callback.");

        cctx.events().removeListener(discoLsnr);

        // Acquire write busy lock.
        busyLock.writeLock().lock();

        // Finish all exchange futures.
        for (GridDhtPartitionsExchangeFuture<K, V> f : exchFuts.values())
            f.onDone(new GridInterruptedException("Grid is stopping: " + cctx.gridName()));

        if (supplyPool != null)
            supplyPool.stop();

        if (demandPool != null)
            demandPool.stop();

        top = null;
        exchFuts = null;
    }

    /**
     * @return Start future.
     */
    @Override public GridFuture<?> startFuture() {
        return startFut;
    }

    /** {@inheritDoc} */
    @Override public GridFuture<?> syncFuture() {
        return demandPool.syncFuture();
    }

    /**
     * @return Exchange futures.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    public List<GridFuture<?>> exchangeFutures() {
        return (List<GridFuture<?>>)(List)exchFuts.values();
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to add.
     */
    public void addDhtAssignmentFetchFuture(long topVer, GridDhtAssignmentFetchFuture<K, V> fut) {
        GridDhtAssignmentFetchFuture<K, V> old = pendingAssignmentFetchFuts.putIfAbsent(topVer, fut);

        assert old == null : "More than one thread is trying to fetch partition assignments: " + topVer;
    }

    /**
     * @param topVer Requested topology version.
     * @param fut Future to remove.
     */
    public void removeDhtAssignmentFetchFuture(long topVer, GridDhtAssignmentFetchFuture<K, V> fut) {
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
    private void processForceKeysRequest(final GridNode node, final GridDhtForceKeysRequest<K, V> msg) {
        GridFuture<?> fut = cctx.mvcc().finishKeys(msg.keys(), msg.topologyVersion());

        if (fut.isDone())
            processForceKeysRequest0(node, msg);
        else
            fut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> t) {
                    processForceKeysRequest0(node, msg);
                }
            });
    }

    /**
     * @param node Node originated request.
     * @param msg Force keys message.
     */
    private void processForceKeysRequest0(GridNode node, GridDhtForceKeysRequest<K, V> msg) {
        if (!enterBusy())
            return;

        try {
            GridNode loc = cctx.localNode();

            GridDhtForceKeysResponse<K, V> res = new GridDhtForceKeysResponse<>(msg.futureId(), msg.miniId());

            for (K k : msg.keys()) {
                int p = cctx.affinity().partition(k);

                GridDhtLocalPartition<K, V> locPart = top.localPartition(p, -1, false);

                // If this node is no longer an owner.
                if (locPart == null && !top.owners(p).contains(loc))
                    res.addMissed(k);

                GridCacheEntryEx<K, V> entry = cctx.dht().peekEx(k);

                // If entry is null, then local partition may have left
                // after the message was received. In that case, we are
                // confident that primary node knows of any changes to the key.
                if (entry != null) {
                    GridCacheEntryInfo<K, V> info = entry.info();

                    if (info != null && !info.isNew())
                        res.addInfo(info);
                }
                else if (log.isDebugEnabled())
                    log.debug("Key is not present in DHT cache: " + k);
            }

            if (log.isDebugEnabled())
                log.debug("Sending force key response [node=" + node.id() + ", res=" + res + ']');

            cctx.io().send(node, res);
        }
        catch (GridTopologyException ignore) {
            if (log.isDebugEnabled())
                log.debug("Received force key request form failed node (will ignore) [nodeId=" + node.id() +
                    ", req=" + msg + ']');
        }
        catch (GridException e) {
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
    private void processForceKeyResponse(GridNode node, GridDhtForceKeysResponse<K, V> msg) {
        if (!enterBusy())
            return;

        try {
            GridDhtForceKeysFuture<K, V> f = forceKeyFuts.get(msg.futureId());

            if (f != null) {
                f.onResult(node.id(), msg);
            }
            else if (log.isDebugEnabled())
                log.debug("Receive force key response for unknown future (is it duplicate?) [nodeId=" + node.id() +
                    ", res=" + msg + ']');
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionRequest(GridNode node, GridDhtPartitionsSingleRequest<K, V> msg) {
        if (!enterBusy())
            return;

        try {
            try {
                sendLocalPartitions(node, msg.exchangeId());
            }
            catch (GridException e) {
                U.error(log, "Failed to send local partition map to node [nodeId=" + node.id() + ", exchId=" +
                    msg.exchangeId() + ']', e);
            }
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node.
     * @param req Request.
     */
    private void processAffinityAssignmentRequest(final GridNode node,
        final GridDhtAffinityAssignmentRequest<K, V> req) {
        final long topVer = req.topologyVersion();

        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment request [node=" + node + ", req=" + req + ']');

        cctx.affinity().affinityReadyFuture(req.topologyVersion()).listenAsync(new CI1<GridFuture<Long>>() {
            @Override public void apply(GridFuture<Long> fut) {
                if (log.isDebugEnabled())
                    log.debug("Affinity is ready for topology version, will send response [topVer=" + topVer +
                        ", node=" + node + ']');

                List<List<GridNode>> assignment = cctx.affinity().assignments(topVer);

                try {
                    cctx.io().send(node, new GridDhtAffinityAssignmentResponse<K, V>(topVer, assignment),
                        AFFINITY_POOL);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send affinity assignment response to remote node [node=" + node + ']', e);
                }
            }
        });
    }

    /**
     * @param node Node.
     * @param res Response.
     */
    private void processAffinityAssignmentResponse(GridNode node, GridDhtAffinityAssignmentResponse<K, V> res) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment response [node=" + node + ", res=" + res + ']');

        for (GridDhtAssignmentFetchFuture<K, V> fut : pendingAssignmentFetchFuts.values())
            fut.onResponse(node, res);
    }

    /**
     * Resends partitions on partition evict within configured timeout.
     *
     * @param part Evicted partition.
     * @param updateSeq Update sequence.
     */
    public void onPartitionEvicted(GridDhtLocalPartition<K, V> part, boolean updateSeq) {
        top.onEvicted(part, updateSeq);

        if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_PART_UNLOADED))
            cctx.events().addUnloadEvent(part.id());

        if (updateSeq)
            demandPool.scheduleResendPartitions();
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    private void processFullPartitionUpdate(GridNode node, GridDhtPartitionsFullMessage<K, V> msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received full partition update [node=" + node.id() + ", msg=" + msg + ']');

                if (top.update(null, msg.partitions()) != null)
                    demandPool.resendPartitions();
            }
            else
                exchangeFuture(msg.exchangeId(), null).onReceive(node.id(), msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param node Node ID.
     * @param msg Message.
     */
    private void processSinglePartitionUpdate(GridNode node, GridDhtPartitionsSingleMessage<K, V> msg) {
        if (!enterBusy())
            return;

        try {
            if (msg.exchangeId() == null) {
                if (log.isDebugEnabled())
                    log.debug("Received local partition update [nodeId=" + node.id() + ", parts=" +
                        msg.partitions().toFullString() + ']');

                if (top.update(null, msg.partitions()) != null)
                    demandPool.scheduleResendPartitions();
            }
            else
                exchangeFuture(msg.exchangeId(), null).onReceive(node.id(), msg);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * Partition refresh callback.
     *
     * @throws GridInterruptedException If interrupted.
     */
    void refreshPartitions() throws GridInterruptedException {
        GridNode oldest = CU.oldest(cctx);

        if (log.isDebugEnabled())
            log.debug("Refreshing partitions [oldest=" + oldest.id() + ", loc=" + cctx.nodeId() + ']');

        Collection<GridNode> rmts = null;

        try {
            // If this is the oldest node.
            if (oldest.id().equals(cctx.nodeId())) {
                rmts = CU.remoteNodes(cctx);

                GridDhtPartitionFullMap map = top.partitionMap(true);

                if (log.isDebugEnabled())
                    log.debug("Refreshing partitions from oldest node: " + map.toFullString());

                sendAllPartitions(rmts, map);
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Refreshing local partitions from non-oldest node: " +
                        top.localPartitionMap().toFullString());

                sendLocalPartitions(oldest, null);
            }
        }
        catch (GridInterruptedException e) {
            throw e;
        }
        catch (GridException e) {
            U.error(log, "Failed to refresh partition map [oldest=" + oldest.id() + ", rmts=" + U.nodeIds(rmts) +
                ", loc=" + cctx.nodeId() + ']', e);
        }
    }

    /**
     * @param nodes Nodes.
     * @param map Partition map.
     * @return {@code True} if message was sent, {@code false} if node left grid.
     * @throws GridException If failed.
     */
    private boolean sendAllPartitions(Collection<? extends GridNode> nodes, GridDhtPartitionFullMap map)
        throws GridException {
        GridDhtPartitionsFullMessage<K, V> m = new GridDhtPartitionsFullMessage<>(null, map, null, -1);

        if (log.isDebugEnabled())
            log.debug("Sending all partitions [nodeIds=" + U.nodeIds(nodes) + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, null);

        return true;
    }

    /**
     * @param node Node.
     * @param id ID.
     * @return {@code True} if message was sent, {@code false} if node left grid.
     * @throws GridException If failed.
     */
    private boolean sendLocalPartitions(GridNode node, @Nullable GridDhtPartitionExchangeId id)
        throws GridException {
        GridDhtPartitionsSingleMessage<K, V> m = new GridDhtPartitionsSingleMessage<>(id, top.localPartitionMap(),
            cctx.versions().last());

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", msg=" + m + ']');

        try {
            cctx.io().send(node, m);

            return true;
        }
        catch (GridTopologyException ignore) {
            if (log.isDebugEnabled())
                log.debug("Failed to send partition update to node because it left grid (will ignore) [node=" +
                    node.id() + ", msg=" + m + ']');

            return false;
        }
    }

    /**
     * @param nodeId Cause node ID.
     * @param topVer Topology version.
     * @param evt Event type.
     * @return Activity future ID.
     */
    private GridDhtPartitionExchangeId exchangeId(UUID nodeId, long topVer, int evt) {
        return new GridDhtPartitionExchangeId(nodeId, evt, topVer);
    }

    /**
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     * @return Exchange future.
     */
    GridDhtPartitionsExchangeFuture<K, V> exchangeFuture(GridDhtPartitionExchangeId exchId,
        @Nullable GridDiscoveryEvent discoEvt) {
        GridDhtPartitionsExchangeFuture<K, V> fut;

        GridDhtPartitionsExchangeFuture<K, V> old = exchFuts.addx(
            fut = new GridDhtPartitionsExchangeFuture<>(cctx, busyLock, exchId));

        if (old != null)
            fut = old;

        if (discoEvt != null)
            fut.onEvent(exchId, discoEvt);

        return fut;
    }

    /**
     * @param exchFut Exchange.
     */
    public void onExchangeDone(GridDhtPartitionsExchangeFuture<K, V> exchFut) {
        assert exchFut.isDone();

        for (GridDhtPartitionsExchangeFuture<K, V> fut : exchFuts.values()) {
            if (fut.exchangeId().topologyVersion() < exchFut.exchangeId().topologyVersion() - 10)
                fut.cleanUp();
        }
    }

    /**
     * @param keys Keys to request.
     * @return Future for request.
     */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public GridDhtFuture<Object> request(Collection<? extends K> keys, long topVer) {
        final GridDhtForceKeysFuture<K, V> fut = new GridDhtForceKeysFuture<>(cctx, topVer, keys, this);

        GridFuture<?> topReadyFut = cctx.affinity().affinityReadyFuturex(topVer);

        if (startFut.isDone() && topReadyFut == null)
            fut.init();
        else {
            if (topReadyFut == null)
                startFut.listenAsync(new CI1<GridFuture<?>>() {
                    @Override public void apply(GridFuture<?> syncFut) {
                        fut.init();
                    }
                });
            else {
                GridCompoundFuture<Object, Object> compound = new GridCompoundFuture<>(cctx.kernalContext());

                compound.add((GridFuture<Object>)startFut);
                compound.add((GridFuture<Object>)topReadyFut);

                compound.markInitialized();

                compound.listenAsync(new CI1<GridFuture<?>>() {
                    @Override public void apply(GridFuture<?> syncFut) {
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
    void addFuture(GridDhtForceKeysFuture<K, V> fut) {
        forceKeyFuts.put(fut.futureId(), fut);
    }

    /**
     * Removes future from future map.
     *
     * @param fut Future to remove.
     */
    void remoteFuture(GridDhtForceKeysFuture<K, V> fut) {
        forceKeyFuts.remove(fut.futureId(), fut);
    }

    /**
     *
     */
    private abstract class MessageHandler<M> implements GridBiInClosure<UUID, M> {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void apply(UUID nodeId, M msg) {
            GridNode node = cctx.node(nodeId);

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
        protected abstract void onMessage(GridNode node, M msg);
    }

    /**
     *
     */
    private class ExchangeFutureSet extends GridListSet<GridDhtPartitionsExchangeFuture<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Creates ordered, not strict list set.
         */
        private ExchangeFutureSet() {
            super(new Comparator<GridDhtPartitionsExchangeFuture<K, V>>() {
                @Override public int compare(
                    GridDhtPartitionsExchangeFuture<K, V> f1,
                    GridDhtPartitionsExchangeFuture<K, V> f2) {
                    long t1 = f1.exchangeId().topologyVersion();
                    long t2 = f2.exchangeId().topologyVersion();

                    assert t1 > 0;
                    assert t2 > 0;

                    // Reverse order.
                    return t1 < t2 ? 1 : t1 == t2 ? 0 : -1;
                }
            }, /*not strict*/false);
        }

        /**
         * @param fut Future to add.
         * @return {@code True} if added.
         */
        @Override public synchronized GridDhtPartitionsExchangeFuture<K, V> addx(
            GridDhtPartitionsExchangeFuture<K, V> fut) {
            GridDhtPartitionsExchangeFuture<K, V> cur = super.addx(fut);

            while (size() > EXCHANGE_HISTORY_SIZE)
                removeLast();

            // Return the value in the set.
            return cur == null ? fut : cur;
        }

        /** {@inheritDoc} */
        @Nullable @Override public synchronized GridDhtPartitionsExchangeFuture<K, V> removex(
            GridDhtPartitionsExchangeFuture<K, V> val
        ) {
            return super.removex(val);
        }

        /**
         * @return Values.
         */
        @Override public synchronized List<GridDhtPartitionsExchangeFuture<K, V>> values() {
            return super.values();
        }

        /** {@inheritDoc} */
        @Override public synchronized String toString() {
            return S.toString(ExchangeFutureSet.class, this, super.toString());
        }
    }
}
