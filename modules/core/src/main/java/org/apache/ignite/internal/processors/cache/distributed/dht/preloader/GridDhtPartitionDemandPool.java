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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;
import static org.apache.ignite.internal.processors.dr.GridDrType.*;

/**
 * Thread pool for requesting partitions from other nodes
 * and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemandPool<K, V> {
    /** Dummy message to wake up a blocking queue if a node leaves. */
    private final SupplyMessage<K, V> DUMMY_TOP = new SupplyMessage<>();

    /** */
    private final GridCacheContext<K, V> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** */
    @GridToStringInclude
    private final Collection<DemandWorker> dmdWorkers;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /** Future for preload mode {@link org.apache.ignite.cache.CachePreloadMode#SYNC}. */
    @GridToStringInclude
    private SyncFuture syncFut;

    /** Preload timeout. */
    private final AtomicLong timeout;

    /** Allows demand threads to synchronize their step. */
    private CyclicBarrier barrier;

    /** Demand lock. */
    private final ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /** */
    private int poolSize;

    /** Last timeout object. */
    private AtomicReference<GridTimeoutObject> lastTimeoutObj = new AtomicReference<>();

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture<K, V> lastExchangeFut;

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

        timeout = new AtomicLong(cctx.config().getPreloadTimeout());
    }

    /**
     *
     */
    void start() {
        if (poolSize > 0) {
            for (DemandWorker w : dmdWorkers)
                new IgniteThread(cctx.gridName(), "preloader-demand-worker", w).start();
        }
    }

    /**
     *
     */
    void stop() {
        U.cancel(dmdWorkers);

        if (log.isDebugEnabled())
            log.debug("Before joining on demand workers: " + dmdWorkers);

        U.join(dmdWorkers, log);

        if (log.isDebugEnabled())
            log.debug("After joining on demand workers: " + dmdWorkers);

        lastExchangeFut = null;

        lastTimeoutObj.set(null);
    }

    /**
     * @return Future for {@link org.apache.ignite.cache.CachePreloadMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return Size of this thread pool.
     */
    int poolSize() {
        return poolSize;
    }

    /**
     * Wakes up demand workers when new exchange future was added.
     */
    void onExchangeFutureAdded() {
        synchronized (dmdWorkers) {
            for (DemandWorker w : dmdWorkers)
                w.addMessage(DUMMY_TOP);
        }
    }

    /**
     * Force preload.
     */
    void forcePreload() {
        GridTimeoutObject obj = lastTimeoutObj.getAndSet(null);

        if (obj != null)
            cctx.time().removeTimeoutObject(obj);

        final GridDhtPartitionsExchangeFuture<K, V> exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing preload event for future: " + exchFut);

            exchFut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                @Override public void apply(IgniteInternalFuture<Long> t) {
                    cctx.shared().exchange().forcePreloadExchange(exchFut);
                }
            });
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force preload request (no topology event happened yet).");
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
    private void preloadEvent(int type, DiscoveryEvent discoEvt) {
        preloadEvent(-1, type, discoEvt);
    }

    /**
     * @param part Partition.
     * @param type Type.
     * @param discoEvt Discovery event.
     */
    private void preloadEvent(int part, int type, DiscoveryEvent discoEvt) {
        assert discoEvt != null;

        cctx.events().addPreloadEvent(part, type, discoEvt.eventNode(), discoEvt.type(), discoEvt.timestamp());
    }

    /**
     * @param msg Message to check.
     * @return {@code True} if dummy message.
     */
    private boolean dummyTopology(SupplyMessage<K, V> msg) {
        return msg == DUMMY_TOP;
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
     * @param p Partition.
     * @param topVer Topology version.
     * @return Picked owners.
     */
    private Collection<ClusterNode> pickedOwners(int p, long topVer) {
        Collection<ClusterNode> affNodes = cctx.affinity().nodes(p, topVer);

        int affCnt = affNodes.size();

        Collection<ClusterNode> rmts = remoteOwners(p, topVer);

        int rmtCnt = rmts.size();

        if (rmtCnt <= affCnt)
            return rmts;

        List<ClusterNode> sorted = new ArrayList<>(rmts);

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
    private Collection<ClusterNode> remoteOwners(int p, long topVer) {
        return F.view(cctx.dht().topology().owners(p, topVer), F.remoteNodes(cctx.nodeId()));
    }

    /**
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     */
    void addAssignments(final GridDhtPreloaderAssignments<K, V> assigns, boolean force) {
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

            final GridDhtPartitionsExchangeFuture<K, V> exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying preload process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                        @Override public void apply(IgniteInternalFuture<Long> f) {
                            cctx.shared().exchange().forcePreloadExchange(exchFut);
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
            cctx.deploy().unwind(cctx);
        }
        finally {
            demandLock.writeLock().unlock();
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
        private final LinkedBlockingDeque<GridDhtPreloaderAssignments<K, V>> assignQ = new LinkedBlockingDeque<>();

        /** Message queue. */
        private final LinkedBlockingDeque<SupplyMessage<K, V>> msgQ =
            new LinkedBlockingDeque<>();

        /** Counter. */
        private long cntr;

        /** Hide worker logger and use cache logger instead. */
        private IgniteLogger log = GridDhtPartitionDemandPool.this.log;

        /**
         * @param id Worker ID.
         */
        private DemandWorker(int id) {
            super(cctx.gridName(), "preloader-demand-worker", GridDhtPartitionDemandPool.this.log);

            assert id >= 0;

            this.id = id;
        }

        /**
         * @param assigns Assignments.
         */
        void addAssignments(GridDhtPreloaderAssignments<K, V> assigns) {
            assert assigns != null;

            assignQ.offer(assigns);

            if (log.isDebugEnabled())
                log.debug("Added assignments to worker: " + this);
        }

        /**
         * @return {@code True} if topology changed.
         */
        private boolean topologyChanged() {
            return !assignQ.isEmpty() || cctx.shared().exchange().topologyChanged();
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
         * @throws IgniteInterruptedCheckedException If interrupted.
         */
        private boolean preloadEntry(ClusterNode pick, int p, GridCacheEntryInfo<K, V> entry, long topVer)
            throws IgniteCheckedException {
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
                                    (IgniteUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, entry.value(), true, null,
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
            catch (IgniteInterruptedCheckedException e) {
                throw e;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteCheckedException("Failed to cache preloaded entry (will stop preloading) [local=" +
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
         * @throws ClusterTopologyCheckedException If node left.
         * @throws IgniteCheckedException If failed to send message.
         */
        private Set<Integer> demandFromNode(ClusterNode node, final long topVer, GridDhtPartitionDemandMessage<K, V> d,
            GridDhtPartitionsExchangeFuture<K, V> exchFut) throws InterruptedException, IgniteCheckedException {
            GridDhtPartitionTopology<K, V> top = cctx.dht().topology();

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
                    cctx.io().send(node, d, cctx.ioPolicy());

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
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                int preloadOrder = cctx.config().getPreloadOrder();

                if (preloadOrder > 0) {
                    IgniteInternalFuture<?> fut = cctx.kernalContext().cache().orderedPreloadFuture(preloadOrder);

                    try {
                        if (fut != null) {
                            if (log.isDebugEnabled())
                                log.debug("Waiting for dependant caches preload [cacheName=" + cctx.name() +
                                    ", preloadOrder=" + preloadOrder + ']');

                            fut.get();
                        }
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for ordered preload future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ", preloadOrder=" + preloadOrder + ']');

                        return;
                    }
                    catch (IgniteCheckedException e) {
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
                    GridDhtPreloaderAssignments<K, V> assigns = null;

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
                            for (ClusterNode node : assigns.keySet()) {
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
                                catch (IgniteInterruptedCheckedException e) {
                                    throw e;
                                }
                                catch (ClusterTopologyCheckedException e) {
                                    if (log.isDebugEnabled())
                                        log.debug("Node left during preloading (will retry) [node=" + node.id() +
                                            ", msg=" + e.getMessage() + ']');

                                    resync = true;

                                    break; // For.
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to receive partitions from node (preloading will not " +
                                        "fully finish) [node=" + node.id() + ", msg=" + d + ']', e);
                                }
                            }

                            // Processed missed entries.
                            if (!missed.isEmpty()) {
                                if (log.isDebugEnabled())
                                    log.debug("Reassigning partitions that were missed: " + missed);

                                assert exchFut.exchangeId() != null;

                                cctx.shared().exchange().forceDummyExchange(true, exchFut);
                            }
                            else
                                break; // While.
                        }
                    }
                    finally {
                        demandLock.readLock().unlock();

                        syncFut.onWorkerDone(this);
                    }

                    cctx.shared().exchange().scheduleResendPartitions();
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
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture<K, V> lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @param exchFut Exchange future.
     * @return Assignments of partitions to nodes.
     */
    GridDhtPreloaderAssignments<K, V> assign(GridDhtPartitionsExchangeFuture<K, V> exchFut) {
        // No assignments for disabled preloader.
        GridDhtPartitionTopology<K, V> top = cctx.dht().topology();

        if (!cctx.preloadEnabled())
            return new GridDhtPreloaderAssignments<>(exchFut, top.topologyVersion());

        int partCnt = cctx.affinity().partitions();

        assert exchFut.forcePreload() || exchFut.dummyReassign() ||
            exchFut.exchangeId().topologyVersion() == top.topologyVersion() :
            "Topology version mismatch [exchId=" + exchFut.exchangeId() +
                ", topVer=" + top.topologyVersion() + ']';

        GridDhtPreloaderAssignments<K, V> assigns = new GridDhtPreloaderAssignments<>(exchFut, top.topologyVersion());

        long topVer = assigns.topologyVersion();

        for (int p = 0; p < partCnt; p++) {
            if (cctx.shared().exchange().hasPendingExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping assignments creation, exchange worker has pending assignments: " +
                        exchFut.exchangeId());

                break;
            }

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

                Collection<ClusterNode> picked = pickedOwners(p, topVer);

                if (picked.isEmpty()) {
                    top.own(part);

                    if (log.isDebugEnabled())
                        log.debug("Owning partition as there are no other owners: " + part);
                }
                else {
                    ClusterNode n = F.first(picked);

                    GridDhtPartitionDemandMessage<K, V> msg = assigns.get(n);

                    if (msg == null) {
                        assigns.put(n, msg = new GridDhtPartitionDemandMessage<>(
                            top.updateSequence(),
                            exchFut.exchangeId().topologyVersion(),
                            cctx.cacheId()));
                    }

                    msg.addPartition(p);
                }
            }
        }

        return assigns;
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
