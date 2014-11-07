/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.dht.GridDhtPartitionState.*;
import static org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.GridDhtPreloader.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Thread pool for requesting partitions from other nodes
 * and populating local cache.
 */
@SuppressWarnings( {"NonConstantFieldWithUpperCaseName"})
public class GridDhtPartitionDemandPool<K, V> {
    /** Dummy message to wake up a blocking queue if a node leaves. */
    private final SupplyMessage<K, V> DUMMY_TOP = new SupplyMessage<>();

    /** */
    private final GridCacheContext<K, V> cctx;

    /** */
    private final GridLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** */
    private GridDhtPartitionTopology<K, V> top;

    /** */
    @GridToStringInclude
    private final Collection<DemandWorker> dmdWorkers;

    /** */
    @GridToStringInclude
    private final ExchangeWorker exchWorker;

    /** Preload predicate. */
    private GridPredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /** Future for preload mode {@link GridCachePreloadMode#SYNC}. */
    @GridToStringInclude
    private SyncFuture syncFut;

    /** Preload timeout. */
    private final AtomicLong timeout;

    /** Last partition refresh. */
    private final AtomicLong lastRefresh = new AtomicLong(-1);

    /** Allows demand threads to synchronize their step. */
    private CyclicBarrier barrier;

    /** Demand lock. */
    private final ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /** */
    private int poolSize;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private AtomicReference<GridDhtPartitionsExchangeFuture<K, V>> lastExchangeFut =
        new AtomicReference<>();

    /** Atomic reference for pending timeout object. */
    private AtomicReference<ResendTimeoutObject> pendingResend = new AtomicReference<>();

    /** Partition resend timeout after eviction. */
    private final long partResendTimeout = getLong(GG_PRELOAD_RESEND_TIMEOUT, DFLT_PRELOAD_RESEND_TIMEOUT);

    /**
     * @param cctx Cache context.
     * @param busyLock Shutdown lock.
     */
    public GridDhtPartitionDemandPool(GridCacheContext<K, V> cctx, ReadWriteLock busyLock) {
        assert cctx != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.busyLock = busyLock;

        log = cctx.logger(getClass());

        top = cctx.dht().topology();

        poolSize = cctx.preloadEnabled() ? cctx.config().getPreloadThreadPoolSize() : 0;

        if (poolSize > 0) {
            barrier = new CyclicBarrier(poolSize);

            dmdWorkers = new ArrayList<>(poolSize);

            for (int i = 0; i < poolSize; i++)
                dmdWorkers.add(new DemandWorker(i));

            syncFut = new SyncFuture(dmdWorkers);
        }
        else {
            dmdWorkers = Collections.emptyList();

            syncFut = new SyncFuture(dmdWorkers);

            // Calling onDone() immediately since preloading is disabled.
            syncFut.onDone();
        }

        exchWorker = new ExchangeWorker();

        timeout = new AtomicLong(cctx.config().getPreloadTimeout());
    }

    /**
     * @param exchFut Exchange future for this node.
     */
    void start(GridDhtPartitionsExchangeFuture<K, V> exchFut) {
        assert exchFut.exchangeId().nodeId().equals(cctx.nodeId());

        if (poolSize > 0) {
            for (DemandWorker w : dmdWorkers)
                new GridThread(cctx.gridName(), "preloader-demand-worker", w).start();
        }

        new GridThread(cctx.gridName(), "exchange-worker", exchWorker).start();

        onDiscoveryEvent(cctx.nodeId(), exchFut);
    }

    /**
     *
     */
    void stop() {
        U.cancel(exchWorker);
        U.cancel(dmdWorkers);

        if (log.isDebugEnabled())
            log.debug("Before joining on exchange worker: " + exchWorker);

        U.join(exchWorker, log);

        if (log.isDebugEnabled())
            log.debug("Before joining on demand workers: " + dmdWorkers);

        U.join(dmdWorkers, log);

        if (log.isDebugEnabled())
            log.debug("After joining on demand workers: " + dmdWorkers);

        ResendTimeoutObject resendTimeoutObj = pendingResend.getAndSet(null);

        if (resendTimeoutObj != null)
            cctx.time().removeTimeoutObject(resendTimeoutObj);

        top = null;
        lastExchangeFut.set(null);
        lastTimeoutObj.set(null);
    }

    /**
     * @return Future for {@link GridCachePreloadMode#SYNC} mode.
     */
    GridFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return Size of this thread pool.
     */
    int poolSize() {
        return poolSize;
    }

    /**
     * Force preload.
     */
    void forcePreload() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            cctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture<K, V> exchFut = lastExchangeFut.get();

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing preload event for future: " + exchFut);

            exchFut.listenAsync(new CI1<GridFuture<Long>>() {
                @Override public void apply(GridFuture<Long> t) {
                    exchWorker.addFuture(forcePreloadExchange(exchFut.discoveryEvent(), exchFut.exchangeId()));
                }
            });
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force preload request (no topology event happened yet).");
    }

    /**
     * Resend partition map.
     */
    void resendPartitions() {
        try {
            refreshPartitions(0);
        }
        catch (GridInterruptedException e) {
            U.warn(log, "Partitions were not refreshed (thread got interrupted): " + e,
                "Partitions were not refreshed (thread got interrupted)");
        }
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (demander is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int type, GridDiscoveryEvent discoEvt) {
        preloadEvent(-1, type, discoEvt);
    }

    /**
     * @param part Partition.
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int part, int type, GridDiscoveryEvent discoEvt) {
        assert discoEvt != null;

        cctx.events().addPreloadEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
    }

    /**
     * @return Dummy node-left message.
     */
    private SupplyMessage<K, V> dummyTopology() {
        return DUMMY_TOP;
    }

    /**
     * @param msg Message to check.
     * @return {@code True} if dummy message.
     */
    private boolean dummyTopology(SupplyMessage<K, V> msg) {
        return msg == DUMMY_TOP;
    }

    /**
     * @param discoEvt Discovery event.
     * @param exchId Exchange ID.
     * @param reassign Dummy reassign flag.
     * @return Dummy partition exchange to handle reassignments if partition topology
     *      changes after preloading started.
     */
    private GridDhtPartitionsExchangeFuture<K, V> dummyExchange(boolean reassign,
        @Nullable GridDiscoveryEvent discoEvt, GridDhtPartitionExchangeId exchId) {
        return new GridDhtPartitionsExchangeFuture<>(cctx, reassign, discoEvt, exchId);
    }

    /**
     * @param discoEvt Discovery event.
     * @param exchId Exchange ID.
     * @return Dummy partition exchange to handle reassignments if partition topology
     *      changes after preloading started.
     */
    private GridDhtPartitionsExchangeFuture<K, V> forcePreloadExchange(
        @Nullable GridDiscoveryEvent discoEvt, GridDhtPartitionExchangeId exchId) {
        return new GridDhtPartitionsExchangeFuture<>(cctx, discoEvt, exchId);
    }

    /**
     * @param exch Exchange.
     * @return {@code True} if dummy exchange.
     */
    private boolean dummyExchange(GridDhtPartitionsExchangeFuture<K, V> exch) {
        return exch.dummy();
    }

    /**
     * @param exch Exchange.
     * @return {@code True} if force preload.
     */
    private boolean forcePreload(GridDhtPartitionsExchangeFuture<K, V> exch) {
        return exch.forcePreload();
    }

    /**
     * @param exch Exchange.
     * @return {@code True} if dummy reassign.
     */
    private boolean dummyReassign(GridDhtPartitionsExchangeFuture<K, V> exch) {
        return (exch.dummy() || exch.forcePreload()) && exch.reassign();
    }

    /**
     * @param nodeId New node ID.
     * @param fut Exchange future.
     */
    void onDiscoveryEvent(UUID nodeId, GridDhtPartitionsExchangeFuture<K, V> fut) {
        if (!enterBusy())
            return;

        try {
            addFuture(fut);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param deque Deque to poll from.
     * @param time Time to wait.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(LinkedBlockingDeque<T> deque, long time, GridWorker w) throws InterruptedException {
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
     * @param deque Deque to poll from.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T take(LinkedBlockingDeque<T> deque, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return deque.take();
    }

    /**
     * @param fut Future.
     * @return {@code True} if added.
     */
    boolean addFuture(GridDhtPartitionsExchangeFuture<K, V> fut) {
        if (fut.onAdded()) {
            exchWorker.addFuture(fut);

            synchronized (dmdWorkers) {
                for (DemandWorker w : dmdWorkers)
                    w.addMessage(DUMMY_TOP);
            }

            return true;
        }

        return false;
    }

    /**
     * Refresh partitions.
     *
     * @param timeout Timeout.
     * @throws GridInterruptedException If interrupted.
     */
    private void refreshPartitions(long timeout) throws GridInterruptedException {
        long last = lastRefresh.get();

        long now = U.currentTimeMillis();

        if (last != -1 && now - last >= timeout && lastRefresh.compareAndSet(last, now)) {
            if (log.isDebugEnabled())
                log.debug("Refreshing partitions [last=" + last + ", now=" + now + ", delta=" + (now - last) +
                    ", timeout=" + timeout + ", lastRefresh=" + lastRefresh + ']');

            cctx.dht().dhtPreloader().refreshPartitions();
        }
        else
        if (log.isDebugEnabled())
            log.debug("Partitions were not refreshed [last=" + last + ", now=" + now + ", delta=" + (now - last) +
                ", timeout=" + timeout + ", lastRefresh=" + lastRefresh + ']');
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Picked owners.
     */
    private Collection<GridNode> pickedOwners(int p, long topVer) {
        Collection<GridNode> affNodes = cctx.affinity().nodes(p, topVer);

        int affCnt = affNodes.size();

        Collection<GridNode> rmts = remoteOwners(p, topVer);

        int rmtCnt = rmts.size();

        if (rmtCnt <= affCnt)
            return rmts;

        List<GridNode> sorted = new ArrayList<>(rmts);

        // Sort in descending order, so nodes with higher order will be first.
        Collections.sort(sorted, CU.nodeComparator(false));

        // Pick newest nodes.
        return sorted.subList(0, affCnt);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return Nodes owning this partition.
     */
    private Collection<GridNode> remoteOwners(int p, long topVer) {
        return F.view(top.owners(p, topVer), F.remoteNodes(cctx.nodeId()));
    }

    /**
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     */
    private void addAssignments(final Assignments assigns, boolean force) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        long delay = cctx.config().getPreloadPartitionedDelay();

        if (delay == 0 || force) {
            assert assigns != null;

            synchronized (dmdWorkers) {
                for (DemandWorker w : dmdWorkers) {
                    w.addAssignments(assigns);

                    w.addMessage(DUMMY_TOP);
                }
            }
        }
        else if (delay > 0) {
            assert !force;

            GridTimeoutObject obj = lastTimeoutObj.get();

            if (obj != null)
                cctx.time().removeTimeoutObject(obj);

            final GridDhtPartitionsExchangeFuture<K, V> exchFut = lastExchangeFut.get();

            assert exchFut != null : "Delaying preload process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listenAsync(new CI1<GridFuture<Long>>() {
                        @Override public void apply(GridFuture<Long> f) {
                            exchWorker.addFuture(forcePreloadExchange(exchFut.discoveryEvent(), exchFut.exchangeId()));
                        }
                    });
                }
            };

            lastTimeoutObj.set(obj);

            cctx.time().addTimeoutObject(obj);
        }
    }

    /**
     *
     */
    void unwindUndeploys() {
        demandLock.writeLock().lock();

        try {
            cctx.deploy().unwind();
        }
        finally {
            demandLock.writeLock().unlock();
        }
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDemandPool.class, this);
    }

    /**
     *
     */
    private class DemandWorker extends GridWorker {
        /** Worker ID. */
        private int id;

        /** Partition-to-node assignments. */
        private final LinkedBlockingDeque<Assignments> assignQ = new LinkedBlockingDeque<>();

        /** Message queue. */
        private final LinkedBlockingDeque<SupplyMessage<K, V>> msgQ =
            new LinkedBlockingDeque<>();

        /** Counter. */
        private long cntr;

        /**
         * @param id Worker ID.
         */
        private DemandWorker(int id) {
            super(cctx.gridName(), "preloader-demand-worker", log);

            assert id >= 0;

            this.id = id;
        }

        /**
         * @param assigns Assignments.
         */
        void addAssignments(Assignments assigns) {
            assert assigns != null;

            assignQ.offer(assigns);

            if (log.isDebugEnabled())
                log.debug("Added assignments to worker: " + this);
        }

        /**
         * @return {@code True} if topology changed.
         */
        private boolean topologyChanged() {
            return !assignQ.isEmpty() || exchWorker.topologyChanged();
        }

        /**
         * @param msg Message.
         */
        private void addMessage(SupplyMessage<K, V> msg) {
            if (!enterBusy())
                return;

            try {
                assert dummyTopology(msg) || msg.supply().workerId() == id;

                msgQ.offer(msg);
            }
            finally {
                leaveBusy();
            }
        }

        /**
         * @param timeout Timed out value.
         */
        private void growTimeout(long timeout) {
            long newTimeout = (long)(timeout * 1.5D);

            // Account for overflow.
            if (newTimeout < 0)
                newTimeout = Long.MAX_VALUE;

            // Grow by 50% only if another thread didn't do it already.
            if (GridDhtPartitionDemandPool.this.timeout.compareAndSet(timeout, newTimeout))
                U.warn(log, "Increased preloading message timeout from " + timeout + "ms to " +
                    newTimeout + "ms.");
        }

        /**
         * @param pick Node picked for preloading.
         * @param p Partition.
         * @param entry Preloaded entry.
         * @param topVer Topology version.
         * @return {@code False} if partition has become invalid during preloading.
         * @throws GridInterruptedException If interrupted.
         */
        private boolean preloadEntry(GridNode pick, int p, GridCacheEntryInfo<K, V> entry, long topVer)
            throws GridException, GridInterruptedException {
            try {
                GridCacheEntryEx<K, V> cached = null;

                try {
                    cached = cctx.dht().entryEx(entry.key());

                    if (log.isDebugEnabled())
                        log.debug("Preloading key [key=" + entry.key() + ", part=" + p + ", node=" + pick.id() + ']');

                    if (cctx.dht().isGgfsDataCache() &&
                        cctx.dht().ggfsDataSpaceUsed() > cctx.dht().ggfsDataSpaceMax()) {
                        LT.error(log, null, "Failed to preload GGFS data cache (GGFS space size exceeded maximum " +
                            "value, will ignore preload entries): " + name());

                        if (cached.markObsoleteIfEmpty(null))
                            cached.context().cache().removeIfObsolete(cached.key());

                        return true;
                    }

                    if (preloadPred == null || preloadPred.apply(entry)) {
                        if (cached.initialValue(
                            entry.value(),
                            entry.valueBytes(),
                            entry.version(),
                            entry.ttl(),
                            entry.expireTime(),
                            true,
                            topVer,
                            cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE
                        )) {
                            cctx.evicts().touch(cached, topVer); // Start tracking.

                            if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_OBJECT_LOADED) && !cached.isInternal())
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                    (GridUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, entry.value(), true, null,
                                    false, null, null, null);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Preloading entry is already in cache (will ignore) [key=" + cached.key() +
                                ", part=" + p + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Preload predicate evaluated to false for entry (will ignore): " + entry);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Entry has been concurrently removed while preloading (will ignore) [key=" +
                            cached.key() + ", part=" + p + ']');
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Partition became invalid during preloading (will ignore): " + p);

                    return false;
                }
            }
            catch (GridInterruptedException e) {
                throw e;
            }
            catch (GridException e) {
                throw new GridException("Failed to cache preloaded entry (will stop preloading) [local=" +
                    cctx.nodeId() + ", node=" + pick.id() + ", key=" + entry.key() + ", part=" + p + ']', e);
            }

            return true;
        }

        /**
         * @param idx Unique index for this topic.
         * @return Topic for partition.
         */
        public Object topic(long idx) {
            return TOPIC_CACHE.topic(cctx.namexx(), cctx.nodeId(), id, idx);
        }

        /**
         * @param node Node to demand from.
         * @param topVer Topology version.
         * @param d Demand message.
         * @param exchFut Exchange future.
         * @return Missed partitions.
         * @throws InterruptedException If interrupted.
         * @throws GridTopologyException If node left.
         * @throws GridException If failed to send message.
         */
        private Set<Integer> demandFromNode(GridNode node, final long topVer, GridDhtPartitionDemandMessage<K, V> d,
            GridDhtPartitionsExchangeFuture<K, V> exchFut) throws InterruptedException, GridException {
            cntr++;

            d.topic(topic(cntr));
            d.workerId(id);

            Set<Integer> missed = new HashSet<>();

            // Get the same collection that will be sent in the message.
            Collection<Integer> remaining = d.partitions();

            // Drain queue before processing a new node.
            drainQueue();

            if (isCancelled() || topologyChanged())
                return missed;

            cctx.io().addOrderedHandler(d.topic(), new CI2<UUID, GridDhtPartitionSupplyMessage<K, V>>() {
                @Override public void apply(UUID nodeId, GridDhtPartitionSupplyMessage<K, V> msg) {
                    addMessage(new SupplyMessage<>(nodeId, msg));
                }
            });

            try {
                boolean retry;

                // DoWhile.
                // =======
                do {
                    retry = false;

                    // Create copy.
                    d = new GridDhtPartitionDemandMessage<>(d, remaining);

                    long timeout = GridDhtPartitionDemandPool.this.timeout.get();

                    d.timeout(timeout);

                    if (log.isDebugEnabled())
                        log.debug("Sending demand message [node=" + node.id() + ", demand=" + d + ']');

                    // Send demand message.
                    cctx.io().send(node, d);

                    // While.
                    // =====
                    while (!isCancelled() && !topologyChanged()) {
                        SupplyMessage<K, V> s = poll(msgQ, timeout, this);

                        // If timed out.
                        if (s == null) {
                            if (msgQ.isEmpty()) { // Safety check.
                                U.warn(log, "Timed out waiting for partitions to load, will retry in " + timeout +
                                    " ms (you may need to increase 'networkTimeout' or 'preloadBatchSize'" +
                                    " configuration properties).");

                                growTimeout(timeout);

                                // Ordered listener was removed if timeout expired.
                                cctx.io().removeOrderedHandler(d.topic());

                                // Must create copy to be able to work with IO manager thread local caches.
                                d = new GridDhtPartitionDemandMessage<>(d, remaining);

                                // Create new topic.
                                d.topic(topic(++cntr));

                                // Create new ordered listener.
                                cctx.io().addOrderedHandler(d.topic(),
                                    new CI2<UUID, GridDhtPartitionSupplyMessage<K, V>>() {
                                        @Override public void apply(UUID nodeId,
                                            GridDhtPartitionSupplyMessage<K, V> msg) {
                                            addMessage(new SupplyMessage<>(nodeId, msg));
                                        }
                                    });

                                // Resend message with larger timeout.
                                retry = true;

                                break; // While.
                            }
                            else
                                continue; // While.
                        }

                        // If topology changed.
                        if (dummyTopology(s)) {
                            if (topologyChanged())
                                break; // While.
                            else
                                continue; // While.
                        }

                        // Check that message was received from expected node.
                        if (!s.senderId().equals(node.id())) {
                            U.warn(log, "Received supply message from unexpected node [expectedId=" + node.id() +
                                ", rcvdId=" + s.senderId() + ", msg=" + s + ']');

                            continue; // While.
                        }

                        if (log.isDebugEnabled())
                            log.debug("Received supply message: " + s);

                        GridDhtPartitionSupplyMessage<K, V> supply = s.supply();

                        // Check whether there were class loading errors on unmarshal
                        if (supply.classError() != null) {
                            if (log.isDebugEnabled())
                                log.debug("Class got undeployed during preloading: " + supply.classError());

                            retry = true;

                            // Quit preloading.
                            break;
                        }

                        // Preload.
                        for (Map.Entry<Integer, Collection<GridCacheEntryInfo<K, V>>> e : supply.infos().entrySet()) {
                            int p = e.getKey();

                            if (cctx.affinity().localNode(p, topVer)) {
                                GridDhtLocalPartition<K, V> part = top.localPartition(p, topVer, true);

                                assert part != null;

                                if (part.state() == MOVING) {
                                    boolean reserved = part.reserve();

                                    assert reserved : "Failed to reserve partition [gridName=" +
                                        cctx.gridName() + ", cacheName=" + cctx.namex() + ", part=" + part + ']';

                                    part.lock();

                                    try {
                                        Collection<Integer> invalidParts = new GridLeanSet<>();

                                        // Loop through all received entries and try to preload them.
                                        for (GridCacheEntryInfo<K, V> entry : e.getValue()) {
                                            if (!invalidParts.contains(p)) {
                                                if (!part.preloadingPermitted(entry.key(), entry.version())) {
                                                    if (log.isDebugEnabled())
                                                        log.debug("Preloading is not permitted for entry due to " +
                                                            "evictions [key=" + entry.key() +
                                                            ", ver=" + entry.version() + ']');

                                                    continue;
                                                }

                                                if (!preloadEntry(node, p, entry, topVer)) {
                                                    invalidParts.add(p);

                                                    if (log.isDebugEnabled())
                                                        log.debug("Got entries for invalid partition during " +
                                                            "preloading (will skip) [p=" + p + ", entry=" + entry + ']');
                                                }
                                            }
                                        }

                                        boolean last = supply.last().contains(p);

                                        // If message was last for this partition,
                                        // then we take ownership.
                                        if (last) {
                                            remaining.remove(p);

                                            top.own(part);

                                            if (log.isDebugEnabled())
                                                log.debug("Finished preloading partition: " + part);

                                            if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_PART_LOADED))
                                                preloadEvent(p, EVT_CACHE_PRELOAD_PART_LOADED,
                                                    exchFut.discoveryEvent());
                                        }
                                    }
                                    finally {
                                        part.unlock();
                                        part.release();
                                    }
                                }
                                else {
                                    remaining.remove(p);

                                    if (log.isDebugEnabled())
                                        log.debug("Skipping loading partition (state is not MOVING): " + part);
                                }
                            }
                            else {
                                remaining.remove(p);

                                if (log.isDebugEnabled())
                                    log.debug("Skipping loading partition (it does not belong on current node): " + p);
                            }
                        }

                        remaining.removeAll(s.supply().missed());

                        // Only request partitions based on latest topology version.
                        for (Integer miss : s.supply().missed())
                            if (cctx.affinity().localNode(miss, topVer))
                                missed.add(miss);

                        if (remaining.isEmpty())
                            break; // While.

                        if (s.supply().ack()) {
                            retry = true;

                            break;
                        }
                    }
                }
                while (retry && !isCancelled() && !topologyChanged());

                return missed;
            }
            finally {
                cctx.io().removeOrderedHandler(d.topic());
            }
        }

        /**
         * @throws InterruptedException If interrupted.
         */
        private void drainQueue() throws InterruptedException {
            while (!msgQ.isEmpty()) {
                SupplyMessage<K, V> msg = msgQ.take();

                if (log.isDebugEnabled())
                    log.debug("Drained supply message: " + msg);
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            try {
                int preloadOrder = cctx.config().getPreloadOrder();

                if (preloadOrder > 0) {
                    GridFuture<?> fut = cctx.kernalContext().cache().orderedPreloadFuture(preloadOrder);

                    try {
                        if (fut != null) {
                            if (log.isDebugEnabled())
                                log.debug("Waiting for dependant caches preload [cacheName=" + cctx.name() +
                                    ", preloadOrder=" + preloadOrder + ']');

                            fut.get();
                        }
                    }
                    catch (GridInterruptedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for ordered preload future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ", preloadOrder=" + preloadOrder + ']');

                        return;
                    }
                    catch (GridException e) {
                        throw new Error("Ordered preload future should never fail: " + e.getMessage(), e);
                    }
                }

                GridDhtPartitionsExchangeFuture<K, V> exchFut = null;

                boolean stopEvtFired = false;

                while (!isCancelled()) {
                    try {
                        barrier.await();

                        if (id == 0 && exchFut != null && !exchFut.dummy() &&
                            cctx.events().isRecordable(EVT_CACHE_PRELOAD_STOPPED)) {

                            if (!cctx.isReplicated() || !stopEvtFired) {
                                preloadEvent(EVT_CACHE_PRELOAD_STOPPED, exchFut.discoveryEvent());

                                stopEvtFired = true;
                            }
                        }
                    }
                    catch (BrokenBarrierException ignore) {
                        throw new InterruptedException("Demand worker stopped.");
                    }

                    // Sync up all demand threads at this step.
                    Assignments assigns = null;

                    while (assigns == null)
                        assigns = poll(assignQ, cctx.gridConfig().getNetworkTimeout(), this);

                    demandLock.readLock().lock();

                    try {
                        exchFut = assigns.exchangeFuture();

                        // Assignments are empty if preloading is disabled.
                        if (assigns.isEmpty())
                            continue;

                        boolean resync = false;

                        // While.
                        // =====
                        while (!isCancelled() && !topologyChanged() && !resync) {
                            Collection<Integer> missed = new HashSet<>();

                            // For.
                            // ===
                            for (GridNode node : assigns.keySet()) {
                                if (topologyChanged() || isCancelled())
                                    break; // For.

                                GridDhtPartitionDemandMessage<K, V> d = assigns.remove(node);

                                // If another thread is already processing this message,
                                // move to the next node.
                                if (d == null)
                                    continue; // For.

                                try {
                                    Set<Integer> set = demandFromNode(node, assigns.topologyVersion(), d, exchFut);

                                    if (!set.isEmpty()) {
                                        if (log.isDebugEnabled())
                                            log.debug("Missed partitions from node [nodeId=" + node.id() + ", missed=" +
                                                set + ']');

                                        missed.addAll(set);
                                    }
                                }
                                catch (GridInterruptedException e) {
                                    throw e;
                                }
                                catch (GridTopologyException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Node left during preloading (will retry) [node=" + node.id() +
                                            ", msg=" + e.getMessage() + ']');

                                    resync = true;

                                    break; // For.
                                }
                                catch (GridException e) {
                                    U.error(log, "Failed to receive partitions from node (preloading will not " +
                                        "fully finish) [node=" + node.id() + ", msg=" + d + ']', e);
                                }
                            }

                            // Processed missed entries.
                            if (!missed.isEmpty()) {
                                if (log.isDebugEnabled())
                                    log.debug("Reassigning partitions that were missed: " + missed);

                                assert exchFut.exchangeId() != null;

                                exchWorker.addFuture(dummyExchange(true, exchFut.discoveryEvent(),
                                    exchFut.exchangeId()));
                            }
                            else
                                break; // While.
                        }
                    }
                    finally {
                        demandLock.readLock().unlock();

                        syncFut.onWorkerDone(this);
                    }

                    scheduleResendPartitions();
                }
            }
            finally {
                // Safety.
                syncFut.onWorkerDone(this);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DemandWorker.class, this, "assignQ", assignQ, "msgQ", msgQ, "super", super.toString());
        }
    }

    /**
     * Partition to node assignments.
     */
    private class Assignments extends ConcurrentHashMap<GridNode, GridDhtPartitionDemandMessage<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Exchange future. */
        @GridToStringExclude
        private final GridDhtPartitionsExchangeFuture<K, V> exchFut;

        /** Last join order. */
        private final long topVer;

        /**
         * @param exchFut Exchange future.
         * @param topVer Last join order.
         */
        Assignments(GridDhtPartitionsExchangeFuture<K, V> exchFut, long topVer) {
            assert exchFut != null;
            assert topVer > 0;

            this.exchFut = exchFut;
            this.topVer = topVer;
        }

        /**
         * @return Exchange future.
         */
        GridDhtPartitionsExchangeFuture<K, V> exchangeFuture() {
            return exchFut;
        }

        /**
         * @return Topology version.
         */
        long topologyVersion() {
            return topVer;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Assignments.class, this, "exchId", exchFut.exchangeId(), "super", super.toString());
        }
    }

    /**
     * Exchange future thread. All exchanges happen only by one thread and next
     * exchange will not start until previous one completes.
     */
    private class ExchangeWorker extends GridWorker {
        /** Future queue. */
        private final LinkedBlockingDeque<GridDhtPartitionsExchangeFuture<K, V>> futQ =
            new LinkedBlockingDeque<>();

        /** Busy flag used as performance optimization to stop current preloading. */
        private volatile boolean busy;

        /**
         *
         */
        private ExchangeWorker() {
            super(cctx.gridName(), "partition-exchanger", log);
        }

        /**
         * @param exchFut Exchange future.
         */
        void addFuture(GridDhtPartitionsExchangeFuture<K, V> exchFut) {
            assert exchFut != null;

            if (!exchFut.dummy() || (futQ.isEmpty() && !busy))
                futQ.offer(exchFut);

            if (log.isDebugEnabled())
                log.debug("Added exchange future to exchange worker: " + exchFut);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            long timeout = cctx.gridConfig().getNetworkTimeout();

            long delay = cctx.config().getPreloadPartitionedDelay();

            boolean startEvtFired = false;

            while (!isCancelled()) {
                GridDhtPartitionsExchangeFuture<K, V> exchFut = null;

                try {
                    // If not first preloading and no more topology events present,
                    // then we periodically refresh partition map.
                    if (futQ.isEmpty() && syncFut.isDone()) {
                        refreshPartitions(timeout);

                        timeout = cctx.gridConfig().getNetworkTimeout();
                    }

                    // After workers line up and before preloading starts we initialize all futures.
                    if (log.isDebugEnabled())
                        log.debug("Before waiting for exchange futures [futs" +
                            F.view((cctx.dht().dhtPreloader()).exchangeFutures(), F.unfinishedFutures()) +
                            ", worker=" + this + ']');

                    // Take next exchange future.
                    exchFut = poll(futQ, timeout, this);

                    if (exchFut == null)
                        continue; // Main while loop.

                    busy = true;

                    Assignments assigns = null;

                    boolean dummyReassign = dummyReassign(exchFut);
                    boolean forcePreload = forcePreload(exchFut);

                    try {
                        if (isCancelled())
                            break;

                        if (!dummyExchange(exchFut) && !forcePreload(exchFut)) {
                            lastExchangeFut.set(exchFut);

                            exchFut.init();

                            exchFut.get();

                            if (log.isDebugEnabled())
                                log.debug("After waiting for exchange future [exchFut=" + exchFut + ", worker=" +
                                    this + ']');

                            if (exchFut.exchangeId().nodeId().equals(cctx.localNodeId()))
                                lastRefresh.compareAndSet(-1, U.currentTimeMillis());

                            // Just pick first worker to do this, so we don't
                            // invoke topology callback more than once for the
                            // same event.
                            if (top.afterExchange(exchFut.exchangeId()) && futQ.isEmpty())
                                resendPartitions(); // Force topology refresh.

                            // Preload event notification.
                            if (cctx.events().isRecordable(EVT_CACHE_PRELOAD_STARTED)) {
                                if (!cctx.isReplicated() || !startEvtFired) {
                                    preloadEvent(EVT_CACHE_PRELOAD_STARTED, exchFut.discoveryEvent());

                                    startEvtFired = true;
                                }
                            }
                        }
                        else {
                            if (log.isDebugEnabled())
                                log.debug("Got dummy exchange (will reassign)");

                            if (!dummyReassign) {
                                timeout = 0; // Force refresh.

                                continue;
                            }
                        }

                        // Don't delay for dummy reassigns to avoid infinite recursion.
                        if (delay == 0 || forcePreload)
                            assigns = assign(exchFut);
                    }
                    finally {
                        // Must flip busy flag before assignments are given to demand workers.
                        busy = false;
                    }

                    addAssignments(assigns, forcePreload);
                }
                catch (GridInterruptedException e) {
                    throw e;
                }
                catch (GridException e) {
                    U.error(log, "Failed to wait for completion of partition map exchange " +
                        "(preloading will not start): " + exchFut, e);
                }
            }
        }

        /**
         * @return {@code True} if another exchange future has been queued up.
         */
        boolean topologyChanged() {
            return !futQ.isEmpty() || busy;
        }

        /**
         * @param exchFut Exchange future.
         * @return Assignments of partitions to nodes.
         */
        private Assignments assign(GridDhtPartitionsExchangeFuture<K, V> exchFut) {
            // No assignments for disabled preloader.
            if (!cctx.preloadEnabled())
                return new Assignments(exchFut, top.topologyVersion());

            int partCnt = cctx.affinity().partitions();

            assert forcePreload(exchFut) || dummyReassign(exchFut) ||
                exchFut.exchangeId().topologyVersion() == top.topologyVersion() :
                "Topology version mismatch [exchId=" + exchFut.exchangeId() + ", topVer=" + top.topologyVersion() + ']';

            Assignments assigns = new Assignments(exchFut, top.topologyVersion());

            long topVer = assigns.topologyVersion();

            for (int p = 0; p < partCnt && !isCancelled() && futQ.isEmpty(); p++) {
                // If partition belongs to local node.
                if (cctx.affinity().localNode(p, topVer)) {
                    GridDhtLocalPartition<K, V> part = top.localPartition(p, topVer, true);

                    assert part != null;
                    assert part.id() == p;

                    if (part.state() != MOVING) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping partition assignment (state is not MOVING): " + part);

                        continue; // For.
                    }

                    Collection<GridNode> picked = pickedOwners(p, topVer);

                    if (picked.isEmpty()) {
                        top.own(part);

                        if (log.isDebugEnabled())
                            log.debug("Owning partition as there are no other owners: " + part);
                    }
                    else {
                        GridNode n = F.first(picked);

                        GridDhtPartitionDemandMessage<K, V> msg = assigns.get(n);

                        if (msg == null)
                            msg = F.addIfAbsent(assigns, n,
                                new GridDhtPartitionDemandMessage<K, V>(top.updateSequence(),
                                    exchFut.exchangeId().topologyVersion()));

                        msg.addPartition(p);
                    }
                }
            }

            return assigns;
        }
    }

    /**
     * Partition resend timeout object.
     */
    private class ResendTimeoutObject implements GridTimeoutObject {
        /** Timeout ID. */
        private final GridUuid timeoutId = GridUuid.randomUuid();

        /** Timeout start time. */
        private final long createTime = U.currentTimeMillis();

        /** Started flag. */
        private AtomicBoolean started = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public GridUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return createTime + partResendTimeout;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            if (!busyLock.readLock().tryLock())
                return;

            try {
                if (started.compareAndSet(false, true))
                    resendPartitions();
            }
            finally {
                busyLock.readLock().unlock();

                cctx.time().removeTimeoutObject(this);

                pendingResend.compareAndSet(this, null);
            }
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
    private class SyncFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Remaining workers. */
        private Collection<DemandWorker> remaining;

        /**
         * @param workers List of workers.
         */
        private SyncFuture(Collection<DemandWorker> workers) {
            super(cctx.kernalContext());

            assert workers.size() == poolSize();

            remaining = Collections.synchronizedList(new LinkedList<>(workers));
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public SyncFuture() {
            assert false;
        }

        /**
         * @param w Worker who iterated through all partitions.
         */
        void onWorkerDone(DemandWorker w) {
            if (isDone())
                return;

            if (remaining.remove(w))
                if (log.isDebugEnabled())
                    log.debug("Completed full partition iteration for worker [worker=" + w + ']');

            if (remaining.isEmpty()) {
                if (log.isDebugEnabled())
                    log.debug("Completed sync future.");

                onDone();
            }
        }
    }

    /**
     * Supply message wrapper.
     */
    private static class SupplyMessage<K, V> {
        /** Sender ID. */
        private UUID sndId;

        /** Supply message. */
        private GridDhtPartitionSupplyMessage<K, V> supply;

        /**
         * Dummy constructor.
         */
        private SupplyMessage() {
            // No-op.
        }

        /**
         * @param sndId Sender ID.
         * @param supply Supply message.
         */
        SupplyMessage(UUID sndId, GridDhtPartitionSupplyMessage<K, V> supply) {
            this.sndId = sndId;
            this.supply = supply;
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return sndId;
        }

        /**
         * @return Message.
         */
        GridDhtPartitionSupplyMessage<K, V> supply() {
            return supply;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SupplyMessage.class, this);
        }
    }
}
