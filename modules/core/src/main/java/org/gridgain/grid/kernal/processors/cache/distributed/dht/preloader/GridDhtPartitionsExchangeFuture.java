/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Future for exchanging partition maps.
 */
public class GridDhtPartitionsExchangeFuture<K, V> extends GridFutureAdapter<Long>
    implements Comparable<GridDhtPartitionsExchangeFuture<K, V>>, GridDhtTopologyFuture {
    /** */
    private static final long serialVersionUID = 0L;

    /** Dummy flag. */
    private final boolean dummy;

    /** Force preload flag. */
    private final boolean forcePreload;

    /** Dummy reassign flag. */
    private final boolean reassign;

    /** Discovery event. */
    private volatile IgniteDiscoveryEvent discoEvt;

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
    private final GridCacheSharedContext<K, V> cctx;

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
    private AtomicReference<GridDiscoveryTopologySnapshot> topSnapshot =
        new AtomicReference<>();

    /** Last committed cache version before next topology version use. */
    private AtomicReference<GridCacheVersion> lastVer = new AtomicReference<>();

    /**
     * Messages received on non-coordinator are stored in case if this node
     * becomes coordinator.
     */
    private final Map<UUID, GridDhtPartitionsSingleMessage<K, V>> singleMsgs = new ConcurrentHashMap8<>();

    /** Messages received from new coordinator. */
    private final Map<UUID, GridDhtPartitionsFullMessage<K, V>> fullMsgs = new ConcurrentHashMap8<>();

    /** */
    @SuppressWarnings({"FieldCanBeLocal", "UnusedDeclaration"})
    @GridToStringInclude
    private volatile IgniteFuture<?> partReleaseFut;

    /** */
    private final Object mux = new Object();

    /** Logger. */
    private IgniteLogger log;

    /**
     * Dummy future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param reassign Dummy reassign flag.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheSharedContext<K, V> cctx, boolean reassign, IgniteDiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());
        dummy = true;
        forcePreload = false;

        this.exchId = exchId;
        this.reassign = reassign;
        this.discoEvt = discoEvt;
        this.cctx = cctx;

        syncNotify(true);

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
    public GridDhtPartitionsExchangeFuture(GridCacheSharedContext<K, V> cctx, IgniteDiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());
        dummy = false;
        forcePreload = true;

        this.exchId = exchId;
        this.discoEvt = discoEvt;
        this.cctx = cctx;

        reassign = true;

        syncNotify(true);

        onDone(exchId.topologyVersion());
    }

    /**
     * @param cctx Cache context.
     * @param busyLock Busy lock.
     * @param exchId Exchange ID.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheSharedContext<K, V> cctx, ReadWriteLock busyLock,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());

        syncNotify(true);

        assert busyLock != null;
        assert exchId != null;

        dummy = false;
        forcePreload = false;
        reassign = false;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.exchId = exchId;

        log = cctx.logger(getClass());

        // Grab all nodes with order of equal or less than last joined node.
        oldestNode.set(CU.oldest(cctx, exchId.topologyVersion()));

        assert oldestNode.get() != null;

        initFut = new GridFutureAdapter<>(ctx, true);

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [localNode=" + cctx.localNodeId() +
                ", fut=" + this + ']');
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtPartitionsExchangeFuture() {
        assert false;

        dummy = true;
        forcePreload = false;
        reassign = false;

        exchId = null;
        cctx = null;
    }

    /** {@inheritDoc} */
    @Override public GridDiscoveryTopologySnapshot topologySnapshot() throws GridException {
        get();

        if (topSnapshot.get() == null)
            topSnapshot.compareAndSet(null, new GridDiscoveryTopologySnapshot(discoEvt.topologyVersion(),
                discoEvt.topologyNodes()));

        return topSnapshot.get();
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
     * Rechecks topology.
     */
    private void initTopology(GridCacheContext<K, V> cacheCtx) throws GridException {
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
            GridDhtAssignmentFetchFuture<K, V> fetchFut =
                new GridDhtAssignmentFetchFuture<>(cacheCtx, exchId.topologyVersion(), CU.affinityNodes(cacheCtx));

            fetchFut.init();

            List<List<ClusterNode>> affAssignment = fetchFut.get();

            if (log.isDebugEnabled())
                log.debug("Fetched affinity from remote node, initializing affinity assignment [locNodeId=" +
                    cctx.localNodeId() + ", topVer=" + exchId.topologyVersion() + ']');

            cacheCtx.affinity().initializeAffinity(exchId.topologyVersion(), affAssignment);
        }
    }

    /**
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity(GridCacheContext<K, V> cacheCtx) {
        GridCacheAffinityFunction affFunc = cacheCtx.config().getAffinity();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!U.hasAnnotation(affFunc, GridCacheCentralizedAffinityFunction.class))
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<ClusterNode> affNodes = CU.affinityNodes(cacheCtx, exchId.topologyVersion());

        return !exchId.nodeId().equals(cctx.localNodeId()) ||
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
    public void onEvent(GridDhtPartitionExchangeId exchId, IgniteDiscoveryEvent discoEvt) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;

        evtLatch.countDown();
    }

    /**
     * @return Discovery event.
     */
    public IgniteDiscoveryEvent discoveryEvent() {
        return discoEvt;
    }

    /**
     * @return Exchange id.
     */
    GridDhtPartitionExchangeId key() {
        return exchId;
    }

    /**
     * @return Oldest node.
     */
    ClusterNode oldestNode() {
        return oldestNode.get();
    }

    /**
     * @return Exchange ID.
     */
    public GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Init future.
     */
    IgniteFuture<?> initFuture() {
        return initFut;
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
     * @throws GridInterruptedException If interrupted.
     */
    public void init() throws GridInterruptedException {
        assert oldestNode.get() != null;

        if (init.compareAndSet(false, true)) {
            if (isDone())
                return;

            try {
                // Wait for event to occur to make sure that discovery
                // will return corresponding nodes.
                U.await(evtLatch);

                assert discoEvt != null;

                assert exchId.nodeId().equals(discoEvt.eventNode().id());

                for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
                    // Update before waiting for locks.
                    if (!cacheCtx.isLocal())
                        cacheCtx.topology().updateTopologyVersion(exchId, this);
                }

                // Grab all alive remote nodes with order of equal or less than last joined node.
                rmtNodes = new ConcurrentLinkedQueue<>(CU.aliveRemoteCacheNodes(cctx, exchId.topologyVersion()));

                rmtIds = Collections.unmodifiableSet(new HashSet<>(F.nodeIds(rmtNodes)));

                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage<K, V>> m : singleMsgs.entrySet())
                    // If received any messages, process them.
                    onReceive(m.getKey(), m.getValue());

                for (Map.Entry<UUID, GridDhtPartitionsFullMessage<K, V>> m : fullMsgs.entrySet())
                    // If received any messages, process them.
                    onReceive(m.getKey(), m.getValue());

                long topVer = exchId.topologyVersion();

                for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    // Must initialize topology after we get discovery event.
                    initTopology(cacheCtx);

                    cacheCtx.preloader().updateLastExchangeFuture(this);
                }

                IgniteFuture<?> partReleaseFut = cctx.partitionReleaseFuture(topVer);

                // Assign to class variable so it will be included into toString() method.
                this.partReleaseFut = partReleaseFut;

                if (log.isDebugEnabled())
                    log.debug("Before waiting for partition release future: " + this);

                partReleaseFut.get();

                if (log.isDebugEnabled())
                    log.debug("After waiting for partition release future: " + this);

                for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
                    if (cacheCtx.isLocal())
                        continue;

                    // Notify replication manager.
                    if (cacheCtx.isDrEnabled())
                        cacheCtx.dr().beforeExchange(topVer, exchId.isLeft());

                    // Partition release future is done so we can flush the write-behind store.
                    cacheCtx.store().forceFlush();

                    // Process queued undeploys prior to sending/spreading map.
                    cacheCtx.preloader().unwindUndeploys();

                    GridDhtPartitionTopology<K, V> top = cacheCtx.topology();

                    assert topVer == top.topologyVersion() :
                        "Topology version is updated only in this class instances inside single ExchangeWorker thread.";

                    top.beforeExchange(exchId);
                }

                for (GridClientPartitionTopology<K, V> top : cctx.exchange().clientTopologies()) {
                    top.updateTopologyVersion(exchId, this);

                    top.beforeExchange(exchId);
                }
            }
            catch (GridInterruptedException e) {
                onDone(e);

                throw e;
            }
            catch (GridException e) {
                U.error(log, "Failed to reinitialize local partitions (preloading will be stopped): " + exchId, e);

                onDone(e);

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

            if (!U.hasCaches(discoEvt.node()))
                onDone(exchId.topologyVersion());
            else {
                // If this node is not oldest.
                if (!oldestNode.get().id().equals(cctx.localNodeId()))
                    sendPartitions();
                else {
                    boolean allReceived = allReceived();

                    if (allReceived && replied.compareAndSet(false, true)) {
                        if (spreadPartitions())
                            onDone(exchId.topologyVersion());
                    }
                }

                scheduleRecheck();
            }
        }
        else
            assert false : "Skipped init future: " + this;
    }

    /**
     * @param node Node.
     * @param id ID.
     * @throws GridException If failed.
     */
    private void sendLocalPartitions(ClusterNode node, @Nullable GridDhtPartitionExchangeId id) throws GridException {
        GridDhtPartitionsSingleMessage<K, V> m = new GridDhtPartitionsSingleMessage<>(id, cctx.versions().last());

        for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal())
                m.addLocalPartitionMap(cacheCtx.cacheId(), cacheCtx.topology().localPartitionMap());
        }

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().send(node, m);
    }

    /**
     * @param nodes Nodes.
     * @param id ID.
     * @throws GridException If failed.
     */
    private void sendAllPartitions(Collection<? extends ClusterNode> nodes, GridDhtPartitionExchangeId id)
        throws GridException {
        GridDhtPartitionsFullMessage<K, V> m = new GridDhtPartitionsFullMessage<>(id, lastVer.get(),
            id.topologyVersion());

        for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
            if (!cacheCtx.isLocal())
                m.addFullPartitionsMap(cacheCtx.cacheId(), cacheCtx.topology().partitionMap(true));
        }

        for (GridClientPartitionTopology<K, V> top : cctx.exchange().clientTopologies())
            m.addFullPartitionsMap(top.cacheId(), top.partitionMap(true));

        if (log.isDebugEnabled())
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, null);
    }

    /**
     *
     */
    private void sendPartitions() {
        ClusterNode oldestNode = this.oldestNode.get();

        try {
            sendLocalPartitions(oldestNode, exchId);
        }
        catch (ClusterTopologyException ignore) {
            if (log.isDebugEnabled())
                log.debug("Oldest node left during partition exchange [nodeId=" + oldestNode.id() +
                    ", exchId=" + exchId + ']');
        }
        catch (GridException e) {
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
        catch (GridException e) {
            scheduleRecheck();

            U.error(log, "Failed to send full partition map to nodes (will retry after timeout) [nodes=" +
                F.nodeId8s(rmtNodes) + ", exchangeId=" + exchId + ']', e);

            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Long res, Throwable err) {
        if (err == null) {
            for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
                if (!cacheCtx.isLocal())
                    cacheCtx.affinity().cleanUpCache(res - 10);
            }
        }

        cctx.exchange().onExchangeDone(this);

        if (super.onDone(res, err) && !dummy && !forcePreload) {
            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.localNodeId() + ", exchange= " + this + ']');

            initFut.onDone(err == null);

            GridTimeoutObject timeoutObj = this.timeoutObj;

            // Deschedule timeout object.
            if (timeoutObj != null)
                cctx.kernalContext().timeout().removeTimeoutObject(timeoutObj);

            for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts()) {
                if (exchId.event() == IgniteEventType.EVT_NODE_FAILED || exchId.event() == IgniteEventType.EVT_NODE_LEFT)
                    cacheCtx.config().getAffinity().removeNode(exchId.nodeId());
            }

            return true;
        }

        return dummy;
    }

    /**
     * Cleans up resources to avoid excessive memory usage.
     */
    public void cleanUp() {
        topSnapshot.set(null);
        singleMsgs.clear();
        fullMsgs.clear();
        rcvdIds.clear();
        rmtNodes.clear();
        oldestNode.set(null);
        partReleaseFut = null;
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
    public void onReceive(final UUID nodeId, final GridDhtPartitionsSingleMessage<K, V> msg) {
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

            try {
                ClusterNode n = cctx.node(nodeId);

                if (n != null)
                    sendAllPartitions(F.asList(n), exchId);
            }
            catch (GridException e) {
                scheduleRecheck();

                U.error(log, "Failed to send full partition map to node (will retry after timeout) [node=" + nodeId +
                    ", exchangeId=" + exchId + ']', e);
            }
        }
        else {
            initFut.listenAsync(new CI1<IgniteFuture<Boolean>>() {
                @Override public void apply(IgniteFuture<Boolean> t) {
                    try {
                        if (!t.get()) // Just to check if there was an error.
                            return;

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
                                    ", fut=" + this + ']');
                        }
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to initialize exchange future: " + this, e);
                    }
                }
            });
        }
    }

    /**
     * @param nodeId Sender node ID.
     * @param msg Full partition info.
     */
    public void onReceive(final UUID nodeId, final GridDhtPartitionsFullMessage<K, V> msg) {
        assert msg != null;

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future [msg=" + msg + ", fut=" + this + ']');

            return;
        }

        ClusterNode curOldest = oldestNode.get();

        if (!nodeId.equals(curOldest.id())) {
            if (log.isDebugEnabled())
                log.debug("Received full partition map from unexpected node [oldest=" + curOldest.id() +
                    ", unexpectedNodeId=" + nodeId + ']');

            ClusterNode sender = ctx.discovery().node(nodeId);

            if (sender == null) {
                if (log.isDebugEnabled())
                    log.debug("Sender node left grid, will ignore message from unexpected node [nodeId=" + nodeId +
                        ", exchId=" + msg.exchangeId() + ']');

                return;
            }

            // Will process message later if sender node becomes oldest node.
            if (sender.order() > curOldest.order())
                fullMsgs.put(nodeId, msg);

            return;
        }

        assert msg.exchangeId().equals(exchId);

        if (log.isDebugEnabled())
            log.debug("Received full partition map from node [nodeId=" + nodeId + ", msg=" + msg + ']');

        assert exchId.topologyVersion() == msg.topologyVersion();

        initFut.listenAsync(new CI1<IgniteFuture<Boolean>>() {
            @Override public void apply(IgniteFuture<Boolean> t) {
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
    private void updatePartitionFullMap(GridDhtPartitionsFullMessage<K, V> msg) {
        for (Map.Entry<Integer, GridDhtPartitionFullMap> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();

            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

            if (cacheCtx != null)
                cacheCtx.topology().update(exchId, entry.getValue());
            else if (CU.oldest(cctx).isLocal())
                cctx.exchange().clientTopology(cacheId, exchId).update(exchId, entry.getValue());
        }
    }

    /**
     * Updates partition map in all caches.
     *
     * @param msg Partitions single message.
     */
    private void updatePartitionSingleMap(GridDhtPartitionsSingleMessage<K, V> msg) {
        for (Map.Entry<Integer, GridDhtPartitionMap> entry : msg.partitions().entrySet()) {
            Integer cacheId = entry.getKey();
            GridCacheContext<K, V> cacheCtx = cctx.cacheContext(cacheId);

            GridDhtPartitionTopology<K, V> top = cacheCtx != null ? cacheCtx.topology() :
                cctx.exchange().clientTopology(cacheId, exchId);

            top.update(exchId, entry.getValue());
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
            initFut.listenAsync(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> f) {
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

                            ClusterNode newOldest = CU.oldest(cctx, exchId.topologyVersion());

                            // If local node is now oldest.
                            if (newOldest.id().equals(cctx.localNodeId())) {
                                synchronized (mux) {
                                    if (oldestNode.compareAndSet(oldest, newOldest)) {
                                        // If local node is just joining.
                                        if (exchId.nodeId().equals(cctx.localNodeId())) {
                                            try {
                                                for (GridCacheContext<K, V> cacheCtx : cctx.cacheContexts())
                                                    cacheCtx.topology().beforeExchange(exchId);
                                            }
                                            catch (GridException e) {
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

                            if (set) {
                                // If received any messages, process them.
                                for (Map.Entry<UUID, GridDhtPartitionsSingleMessage<K, V>> m : singleMsgs.entrySet())
                                    onReceive(m.getKey(), m.getValue());

                                for (Map.Entry<UUID, GridDhtPartitionsFullMessage<K, V>> m : fullMsgs.entrySet())
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

                            for (Iterator<ClusterNode> it = rmtNodes.iterator(); it.hasNext();)
                                if (it.next().id().equals(nodeId))
                                    it.remove();

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
        // If this is the oldest node.
        if (oldestNode.get().id().equals(cctx.localNodeId())) {
            Collection<UUID> remaining = remaining();

            if (!remaining.isEmpty()) {
                try {
                    cctx.io().safeSend(cctx.discovery().nodes(remaining),
                        new GridDhtPartitionsSingleRequest<K, V>(exchId), null);
                }
                catch (GridException e) {
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
            sendPartitions();

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
                cctx.gridConfig().getNetworkTimeout() * cctx.gridConfig().getCacheConfiguration().length) {
                @Override public void onTimeout() {
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
    @Override public int compareTo(GridDhtPartitionsExchangeFuture<K, V> fut) {
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
