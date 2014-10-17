/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.logger.*;
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

    /** */
    private final GridDhtPartitionTopology<K, V> top;

    /** Discovery event. */
    private volatile GridDiscoveryEvent discoEvt;

    /** */
    @GridToStringInclude
    private final Collection<UUID> rcvdIds = new GridConcurrentHashSet<>();

    /** Remote nodes. */
    private volatile Collection<GridNode> rmtNodes;

    /** Remote nodes. */
    @GridToStringInclude
    private volatile Collection<UUID> rmtIds;

    /** Oldest node. */
    @GridToStringExclude
    private final AtomicReference<GridNode> oldestNode = new AtomicReference<>();

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
    private final GridCacheContext<K, V> cctx;

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
    private volatile GridFuture<?> partReleaseFut;

    /** */
    private final Object mux = new Object();

    /** Logger. */
    private GridLogger log;

    /**
     * Dummy future created to trigger reassignments if partition
     * topology changed while preloading.
     *
     * @param cctx Cache context.
     * @param reassign Dummy reassign flag.
     * @param discoEvt Discovery event.
     * @param exchId Exchange id.
     */
    public GridDhtPartitionsExchangeFuture(GridCacheContext<K, V> cctx, boolean reassign, GridDiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());
        dummy = true;
        forcePreload = false;

        top = null;

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
    public GridDhtPartitionsExchangeFuture(GridCacheContext<K, V> cctx, GridDiscoveryEvent discoEvt,
        GridDhtPartitionExchangeId exchId) {
        super(cctx.kernalContext());
        dummy = false;
        forcePreload = true;

        top = null;

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
    GridDhtPartitionsExchangeFuture(GridCacheContext<K, V> cctx, ReadWriteLock busyLock,
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

        top = cctx.dht().topology();

        // Grab all nodes with order of equal or less than last joined node.
        oldestNode.set(CU.oldest(cctx, exchId.topologyVersion()));

        assert oldestNode.get() != null;

        initFut = new GridFutureAdapter<>(ctx, true);

        if (log.isDebugEnabled())
            log.debug("Creating exchange future [cacheName=" + cctx.namex() + ", localNode=" + cctx.nodeId() +
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

        top = null;
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
    boolean dummy() {
        return dummy;
    }

    /**
     * @return Force preload flag.
     */
    boolean forcePreload() {
        return forcePreload;
    }

    /**
     * @return Dummy reassign flag.
     */
    boolean reassign() {
        return reassign;
    }

    /**
     * Rechecks topology.
     */
    private void initTopology() throws GridException {
        // Grab all alive remote nodes with order of equal or less than last joined node.
        rmtNodes = new ConcurrentLinkedQueue<>(CU.aliveRemoteNodes(cctx, exchId.topologyVersion()));

        rmtIds = Collections.unmodifiableSet(new HashSet<>(F.nodeIds(rmtNodes)));

        for (Map.Entry<UUID, GridDhtPartitionsSingleMessage<K, V>> m : singleMsgs.entrySet()) {
            // If received any messages, process them.
            onReceive(m.getKey(), m.getValue());
        }

        for (Map.Entry<UUID, GridDhtPartitionsFullMessage<K, V>> m : fullMsgs.entrySet()) {
            // If received any messages, process them.
            onReceive(m.getKey(), m.getValue());
        }

        if (canCalculateAffinity()) {
            if (log.isDebugEnabled())
                log.debug("Will recalculate affinity [locNodeId=" + cctx.localNodeId() + ", exchId=" + exchId + ']');

            cctx.affinity().calculateAffinity(exchId.topologyVersion(), discoEvt);
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Will request affinity from remote node [locNodeId=" + cctx.localNodeId() + ", exchId=" +
                    exchId + ']');

            // Fetch affinity assignment from remote node.
            GridDhtAssignmentFetchFuture<K, V> fetchFut =
                new GridDhtAssignmentFetchFuture<>(cctx, exchId.topologyVersion(), CU.affinityNodes(cctx));

            fetchFut.init();

            List<List<GridNode>> affAssignment = fetchFut.get();

            if (log.isDebugEnabled())
                log.debug("Fetched affinity from remote node, initializing affinity assignment [locNodeId=" +
                    cctx.localNodeId() + ", topVer=" + exchId.topologyVersion() + ']');

            cctx.affinity().initializeAffinity(exchId.topologyVersion(), affAssignment);
        }
    }

    /**
     * @return {@code True} if local node can calculate affinity on it's own for this partition map exchange.
     */
    private boolean canCalculateAffinity() {
        GridCacheAffinityFunction affFunc = cctx.config().getAffinity();

        // Do not request affinity from remote nodes if affinity function is not centralized.
        if (!U.hasAnnotation(affFunc, GridCacheCentralizedAffinityFunction.class))
            return true;

        // If local node did not initiate exchange or local node is the only cache node in grid.
        Collection<GridNode> affNodes = CU.affinityNodes(cctx, exchId.topologyVersion());

        return !exchId.nodeId().equals(cctx.localNodeId()) ||
            (affNodes.size() == 1 && affNodes.contains(cctx.localNode()));
    }

    /**
     * @return {@code True}
     */
    boolean onAdded() {
        return added.compareAndSet(false, true);
    }

    /**
     * Event callback.
     *
     * @param exchId Exchange ID.
     * @param discoEvt Discovery event.
     */
    void onEvent(GridDhtPartitionExchangeId exchId, GridDiscoveryEvent discoEvt) {
        assert exchId.equals(this.exchId);

        this.discoEvt = discoEvt;

        evtLatch.countDown();
    }

    /**
     * @return Discovery event.
     */
    GridDiscoveryEvent discoveryEvent() {
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
    GridNode oldestNode() {
        return oldestNode.get();
    }

    /**
     * @return Exchange ID.
     */
    GridDhtPartitionExchangeId exchangeId() {
        return exchId;
    }

    /**
     * @return Init future.
     */
    GridFuture<?> initFuture() {
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
    void init() throws GridInterruptedException {
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

                // Update before waiting for locks.
                top.updateTopologyVersion(exchId, this);

                // Must initialize topology after we get discovery event.
                initTopology();

                long topVer = exchId.topologyVersion();

                assert topVer == top.topologyVersion() :
                    "Topology version is updated only in this class instances inside single ExchangeWorker thread.";

                // Do not wait for partition release future only if local node has just joined the grid.
                if (canCalculateAffinity()) {
                    // Get partitions of event source node: join => new topology, left => previous topology.
                    long moveTopVer = topVer + (exchId.isLeft() ? -1 : 0);

                    Collection<Integer> parts = F.concat(false,
                        cctx.affinity().primaryPartitions(exchId.nodeId(), moveTopVer),
                        cctx.affinity().backupPartitions(exchId.nodeId(), moveTopVer));

                    GridFuture<?> partReleaseFut = cctx.partitionReleaseFuture(parts, topVer);

                    // Assign to class variable so it will be included into toString() method.
                    this.partReleaseFut = partReleaseFut;

                    if (log.isDebugEnabled())
                        log.debug("Before waiting for partition release future: " + this);

                    partReleaseFut.get();

                    if (log.isDebugEnabled())
                        log.debug("After waiting for partition release future: " + this);
                }

                // Notify replication manager.
                if (cctx.isDrEnabled())
                    cctx.dr().beforeExchange(topVer, exchId.isLeft());

                // Partition release future is done so we can flush the write-behind store.
                cctx.store().forceFlush();

                // Process queued undeploys prior to sending/spreading map.
                cctx.preloader().unwindUndeploys();

                top.beforeExchange(exchId);
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

            if (!U.hasCache(discoEvt.node(), cctx.name())) {
                assert canCalculateAffinity();

                onDone(exchId.topologyVersion());
            }
            else {
                // If this node is not oldest.
                if (!oldestNode.get().id().equals(cctx.nodeId()))
                    sendPartitions();
                else {
                    boolean allReceived = allReceived();

                    if (allReceived && replied.compareAndSet(false, true)) {
                        if (spreadPartitions(top.partitionMap(true)))
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
    private void sendLocalPartitions(GridNode node, @Nullable GridDhtPartitionExchangeId id) throws GridException {
        GridDhtPartitionsSingleMessage<K, V> m = new GridDhtPartitionsSingleMessage<>(id, top.localPartitionMap(),
            cctx.versions().last());

        if (log.isDebugEnabled())
            log.debug("Sending local partitions [nodeId=" + node.id() + ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().send(node, m);
    }

    /**
     * @param nodes Nodes.
     * @param id ID.
     * @param partMap Partition map.
     * @throws GridException If failed.
     */
    private void sendAllPartitions(Collection<? extends GridNode> nodes, GridDhtPartitionExchangeId id,
        GridDhtPartitionFullMap partMap) throws GridException {
        GridDhtPartitionsFullMessage<K, V> m = new GridDhtPartitionsFullMessage<>(id, partMap, lastVer.get(),
            id.topologyVersion());

        if (log.isDebugEnabled())
            log.debug("Sending full partition map [nodeIds=" + F.viewReadOnly(nodes, F.node2id()) +
                ", exchId=" + exchId + ", msg=" + m + ']');

        cctx.io().safeSend(nodes, m, null);
    }

    /**
     *
     */
    private void sendPartitions() {
        GridNode oldestNode = this.oldestNode.get();

        try {
            sendLocalPartitions(oldestNode, exchId);
        }
        catch (GridTopologyException ignore) {
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
     * @param partMap Partition map.
     * @return {@code True} if succeeded.
     */
    private boolean spreadPartitions(GridDhtPartitionFullMap partMap) {
        try {
            sendAllPartitions(rmtNodes, exchId, partMap);

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
        if (err == null)
            cctx.affinity().cleanUpCache(res - 10);

        if (super.onDone(res, err) && !dummy && !forcePreload) {
            if (exchId.event() == GridEventType.EVT_NODE_FAILED || exchId.event() == GridEventType.EVT_NODE_LEFT)
                cctx.config().getAffinity().removeNode(exchId.nodeId());

            if (log.isDebugEnabled())
                log.debug("Completed partition exchange [localNode=" + cctx.nodeId() + ", exchange= " + this + ']');

            initFut.onDone(err == null);

            GridTimeoutObject timeoutObj = this.timeoutObj;

            // Deschedule timeout object.
            if (timeoutObj != null)
                cctx.time().removeTimeoutObject(timeoutObj);

            ((GridDhtPreloader<K, V>)cctx.preloader()).onExchangeDone(this);

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
    void onReceive(final UUID nodeId, final GridDhtPartitionsSingleMessage<K, V> msg) {
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
                GridNode n = cctx.node(nodeId);

                if (n != null)
                    sendAllPartitions(F.asList(n), exchId, top.partitionMap(true));
            }
            catch (GridException e) {
                scheduleRecheck();

                U.error(log, "Failed to send full partition map to node (will retry after timeout) [node=" + nodeId +
                    ", exchangeId=" + exchId + ']', e);
            }
        }
        else {
            initFut.listenAsync(new CI1<GridFuture<Boolean>>() {
                @Override public void apply(GridFuture<Boolean> t) {
                    try {
                        if (!t.get()) // Just to check if there was an error.
                            return;

                        GridNode loc = cctx.localNode();

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
                                    top.update(exchId, msg.partitions());

                                allReceived = allReceived();
                            }

                            // If got all replies, and initialization finished, and reply has not been sent yet.
                            if (allReceived && ready.get() && replied.compareAndSet(false, true)) {
                                spreadPartitions(top.partitionMap(true));

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
    void onReceive(final UUID nodeId, final GridDhtPartitionsFullMessage<K, V> msg) {
        assert msg != null;

        if (isDone()) {
            if (log.isDebugEnabled())
                log.debug("Received message for finished future [msg=" + msg + ", fut=" + this + ']');

            return;
        }

        GridNode curOldest = oldestNode.get();

        if (!nodeId.equals(curOldest.id())) {
            if (log.isDebugEnabled())
                log.debug("Received full partition map from unexpected node [oldest=" + curOldest.id() +
                    ", unexpectedNodeId=" + nodeId + ']');

            GridNode sender = ctx.discovery().node(nodeId);

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

        initFut.listenAsync(new CI1<GridFuture<Boolean>>() {
            @Override public void apply(GridFuture<Boolean> t) {
                assert msg.lastVersion() != null;

                cctx.versions().onReceived(nodeId, msg.lastVersion());

                top.update(exchId, msg.partitions());

                onDone(exchId.topologyVersion());
            }
        });
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
            initFut.listenAsync(new CI1<GridFuture<?>>() {
                @Override public void apply(GridFuture<?> f) {
                    if (isDone())
                        return;

                    if (!enterBusy())
                        return;

                    try {
                        // Pretend to have received message from this node.
                        rcvdIds.add(nodeId);

                        Collection<UUID> rmtIds = GridDhtPartitionsExchangeFuture.this.rmtIds;

                        assert rmtIds != null;

                        GridNode oldest = oldestNode.get();

                        if (oldest.id().equals(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Oldest node left or failed on partition exchange " +
                                    "(will restart exchange process)) [cacheName=" + cctx.namex() +
                                    ", oldestNodeId=" + oldest.id() + ", exchangeId=" + exchId + ']');

                            boolean set = false;

                            GridNode newOldest = CU.oldest(cctx, exchId.topologyVersion());

                            // If local node is now oldest.
                            if (newOldest.id().equals(cctx.nodeId())) {
                                synchronized (mux) {
                                    if (oldestNode.compareAndSet(oldest, newOldest)) {
                                        // If local node is just joining.
                                        if (exchId.nodeId().equals(cctx.nodeId())) {
                                            try {
                                                top.beforeExchange(exchId);
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

                            for (Iterator<GridNode> it = rmtNodes.iterator(); it.hasNext();)
                                if (it.next().id().equals(nodeId))
                                    it.remove();

                            if (allReceived() && ready.get() && replied.compareAndSet(false, true))
                                if (spreadPartitions(top.partitionMap(true)))
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
        if (oldestNode.get().id().equals(cctx.nodeId())) {
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
                if (spreadPartitions(top.partitionMap(true)))
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
                cctx.time().removeTimeoutObject(old);

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

            cctx.time().addTimeoutObject(timeoutObj);
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
        GridNode oldestNode = this.oldestNode.get();

        return S.toString(GridDhtPartitionsExchangeFuture.class, this,
            "oldest", oldestNode == null ? "null" : oldestNode.id(),
            "oldestOrder", oldestNode == null ? "null" : oldestNode.order(),
            "evtLatch", evtLatch == null ? "null" : evtLatch.getCount(),
            "remaining", remaining(),
            "super", super.toString());
    }
}
