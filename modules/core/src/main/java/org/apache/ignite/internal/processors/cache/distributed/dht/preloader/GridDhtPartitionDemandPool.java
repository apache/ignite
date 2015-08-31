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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryInfoCollection;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtInvalidPartitionException;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_LOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_NONE;
import static org.apache.ignite.internal.processors.dr.GridDrType.DR_PRELOAD;

/**
 * Thread pool for requesting partitions from other nodes
 * and populating local cache.
 */
@SuppressWarnings("NonConstantFieldWithUpperCaseName")
public class GridDhtPartitionDemandPool {
    /** Dummy message to wake up a blocking queue if a node leaves. */
    private final SupplyMessage DUMMY_TOP = new SupplyMessage();

    /** */
    private final GridCacheContext<?, ?> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** */
    @GridToStringInclude
    private final Collection<DemandWorker> dmdWorkers;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo> preloadPred;

    /** Future for preload mode {@link CacheRebalanceMode#SYNC}. */
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
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /**
     * @param cctx Cache context.
     * @param busyLock Shutdown lock.
     */
    public GridDhtPartitionDemandPool(GridCacheContext<?, ?> cctx, ReadWriteLock busyLock) {
        assert cctx != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.busyLock = busyLock;

        log = cctx.logger(getClass());

        boolean enabled = cctx.rebalanceEnabled() && !cctx.kernalContext().clientNode();

        poolSize = enabled ? cctx.config().getRebalanceThreadPoolSize() : 0;

        if (enabled) {
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

        timeout = new AtomicLong(cctx.config().getRebalanceTimeout());
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
     * @return Future for {@link CacheRebalanceMode#SYNC} mode.
     */
    IgniteInternalFuture<?> syncFuture() {
        return syncFut;
    }

    /**
     * Sets preload predicate for demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(IgnitePredicate<GridCacheEntryInfo> preloadPred) {
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

        final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

        if (exchFut != null) {
            if (log.isDebugEnabled())
                log.debug("Forcing rebalance event for future: " + exchFut);

            exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> t) {
                    cctx.shared().exchange().forcePreloadExchange(exchFut);
                }
            });
        }
        else if (log.isDebugEnabled())
            log.debug("Ignoring force rebalance request (no topology event happened yet).");
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
    private boolean dummyTopology(SupplyMessage msg) {
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
    private Collection<ClusterNode> pickedOwners(int p, AffinityTopologyVersion topVer) {
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
    private Collection<ClusterNode> remoteOwners(int p, AffinityTopologyVersion topVer) {
        return F.view(cctx.dht().topology().owners(p, topVer), F.remoteNodes(cctx.nodeId()));
    }

    /**
     * @param assigns Assignments.
     * @param force {@code True} if dummy reassign.
     */
    void addAssignments(final GridDhtPreloaderAssignments assigns, boolean force) {
        if (log.isDebugEnabled())
            log.debug("Adding partition assignments: " + assigns);

        long delay = cctx.config().getRebalanceDelay();

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

            final GridDhtPartitionsExchangeFuture exchFut = lastExchangeFut;

            assert exchFut != null : "Delaying rebalance process without topology event.";

            obj = new GridTimeoutObjectAdapter(delay) {
                @Override public void onTimeout() {
                    exchFut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                        @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> f) {
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
        private final LinkedBlockingDeque<GridDhtPreloaderAssignments> assignQ = new LinkedBlockingDeque<>();

        /** Message queue. */
        private final LinkedBlockingDeque<SupplyMessage> msgQ =
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
        void addAssignments(GridDhtPreloaderAssignments assigns) {
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
        private void addMessage(SupplyMessage msg) {
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
                U.warn(log, "Increased rebalancing message timeout from " + timeout + "ms to " +
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
        private boolean preloadEntry(
            ClusterNode pick,
            int p,
            GridCacheEntryInfo entry,
            AffinityTopologyVersion topVer
        ) throws IgniteCheckedException {
            try {
                GridCacheEntryEx cached = null;

                try {
                    cached = cctx.dht().entryEx(entry.key());

                    if (log.isDebugEnabled())
                        log.debug("Rebalancing key [key=" + entry.key() + ", part=" + p + ", node=" + pick.id() + ']');

                    if (cctx.dht().isIgfsDataCache() &&
                        cctx.dht().igfsDataSpaceUsed() > cctx.dht().igfsDataSpaceMax()) {
                        LT.error(log, null, "Failed to rebalance IGFS data cache (IGFS space size exceeded maximum " +
                            "value, will ignore rebalance entries): " + name());

                        if (cached.markObsoleteIfEmpty(null))
                            cached.context().cache().removeIfObsolete(cached.key());

                        return true;
                    }

                    if (preloadPred == null || preloadPred.apply(entry)) {
                        if (cached.initialValue(
                            entry.value(),
                            entry.version(),
                            entry.ttl(),
                            entry.expireTime(),
                            true,
                            topVer,
                            cctx.isDrEnabled() ? DR_PRELOAD : DR_NONE
                        )) {
                            cctx.evicts().touch(cached, topVer); // Start tracking.

                            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_LOADED) && !cached.isInternal())
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_REBALANCE_OBJECT_LOADED, entry.value(), true, null,
                                    false, null, null, null);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Rebalancing entry is already in cache (will ignore) [key=" + cached.key() +
                                ", part=" + p + ']');
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Rebalance predicate evaluated to false for entry (will ignore): " + entry);
                }
                catch (GridCacheEntryRemovedException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Entry has been concurrently removed while rebalancing (will ignore) [key=" +
                            cached.key() + ", part=" + p + ']');
                }
                catch (GridDhtInvalidPartitionException ignored) {
                    if (log.isDebugEnabled())
                        log.debug("Partition became invalid during rebalancing (will ignore): " + p);

                    return false;
                }
            }
            catch (IgniteInterruptedCheckedException e) {
                throw e;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteCheckedException("Failed to cache rebalanced entry (will stop rebalancing) [local=" +
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
        private Set<Integer> demandFromNode(
            ClusterNode node,
            final AffinityTopologyVersion topVer,
            GridDhtPartitionDemandMessage d,
            GridDhtPartitionsExchangeFuture exchFut
        ) throws InterruptedException, IgniteCheckedException {
            GridDhtPartitionTopology top = cctx.dht().topology();

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

            cctx.io().addOrderedHandler(d.topic(), new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                @Override public void apply(UUID nodeId, GridDhtPartitionSupplyMessage msg) {
                    addMessage(new SupplyMessage(nodeId, msg));
                }
            });

            try {
                boolean retry;

                // DoWhile.
                // =======
                do {
                    retry = false;

                    // Create copy.
                    d = new GridDhtPartitionDemandMessage(d, remaining);

                    long timeout = GridDhtPartitionDemandPool.this.timeout.get();

                    d.timeout(timeout);

                    if (log.isDebugEnabled())
                        log.debug("Sending demand message [node=" + node.id() + ", demand=" + d + ']');

                    // Send demand message.
                    cctx.io().send(node, d, cctx.ioPolicy());

                    // While.
                    // =====
                    while (!isCancelled() && !topologyChanged()) {
                        SupplyMessage s = poll(msgQ, timeout, this);

                        // If timed out.
                        if (s == null) {
                            if (msgQ.isEmpty()) { // Safety check.
                                U.warn(log, "Timed out waiting for partitions to load, will retry in " + timeout +
                                    " ms (you may need to increase 'networkTimeout' or 'rebalanceBatchSize'" +
                                    " configuration properties).");

                                growTimeout(timeout);

                                // Ordered listener was removed if timeout expired.
                                cctx.io().removeOrderedHandler(d.topic());

                                // Must create copy to be able to work with IO manager thread local caches.
                                d = new GridDhtPartitionDemandMessage(d, remaining);

                                // Create new topic.
                                d.topic(topic(++cntr));

                                // Create new ordered listener.
                                cctx.io().addOrderedHandler(d.topic(),
                                    new CI2<UUID, GridDhtPartitionSupplyMessage>() {
                                        @Override public void apply(UUID nodeId,
                                            GridDhtPartitionSupplyMessage msg) {
                                            addMessage(new SupplyMessage(nodeId, msg));
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

                        GridDhtPartitionSupplyMessage supply = s.supply();

                        // Check whether there were class loading errors on unmarshal
                        if (supply.classError() != null) {
                            if (log.isDebugEnabled())
                                log.debug("Class got undeployed during preloading: " + supply.classError());

                            retry = true;

                            // Quit preloading.
                            break;
                        }

                        // Preload.
                        for (Map.Entry<Integer, CacheEntryInfoCollection> e : supply.infos().entrySet()) {
                            int p = e.getKey();

                            if (cctx.affinity().localNode(p, topVer)) {
                                GridDhtLocalPartition part = top.localPartition(p, topVer, true);

                                assert part != null;

                                if (part.state() == MOVING) {
                                    boolean reserved = part.reserve();

                                    assert reserved : "Failed to reserve partition [gridName=" +
                                        cctx.gridName() + ", cacheName=" + cctx.namex() + ", part=" + part + ']';

                                    part.lock();

                                    try {
                                        Collection<Integer> invalidParts = new GridLeanSet<>();

                                        // Loop through all received entries and try to preload them.
                                        for (GridCacheEntryInfo entry : e.getValue().infos()) {
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
                                                log.debug("Finished rebalancing partition: " + part);

                                            if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_LOADED))
                                                preloadEvent(p, EVT_CACHE_REBALANCE_PART_LOADED,
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
                                        log.debug("Skipping rebalancing partition (state is not MOVING): " + part);
                                }
                            }
                            else {
                                remaining.remove(p);

                                if (log.isDebugEnabled())
                                    log.debug("Skipping rebalancing partition (it does not belong on current node): " + p);
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
                SupplyMessage msg = msgQ.take();

                if (log.isDebugEnabled())
                    log.debug("Drained supply message: " + msg);
            }
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                int rebalanceOrder = cctx.config().getRebalanceOrder();

                if (!CU.isMarshallerCache(cctx.name())) {
                    if (log.isDebugEnabled())
                        log.debug("Waiting for marshaller cache preload [cacheName=" + cctx.name() + ']');

                    try {
                        cctx.kernalContext().cache().marshallerCache().preloader().syncFuture().get();
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for marshaller cache preload future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ']');

                        return;
                    }
                    catch (IgniteCheckedException e) {
                        throw new Error("Ordered preload future should never fail: " + e.getMessage(), e);
                    }
                }

                if (rebalanceOrder > 0) {
                    IgniteInternalFuture<?> fut = cctx.kernalContext().cache().orderedPreloadFuture(rebalanceOrder);

                    try {
                        if (fut != null) {
                            if (log.isDebugEnabled())
                                log.debug("Waiting for dependant caches rebalance [cacheName=" + cctx.name() +
                                    ", rebalanceOrder=" + rebalanceOrder + ']');

                            fut.get();
                        }
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to wait for ordered rebalance future (grid is stopping): " +
                                "[cacheName=" + cctx.name() + ", rebalanceOrder=" + rebalanceOrder + ']');

                        return;
                    }
                    catch (IgniteCheckedException e) {
                        throw new Error("Ordered rebalance future should never fail: " + e.getMessage(), e);
                    }
                }

                GridDhtPartitionsExchangeFuture exchFut = null;

                boolean stopEvtFired = false;

                while (!isCancelled()) {
                    try {
                        barrier.await();

                        if (id == 0 && exchFut != null && !exchFut.dummy() &&
                            cctx.events().isRecordable(EVT_CACHE_REBALANCE_STOPPED)) {

                            if (!cctx.isReplicated() || !stopEvtFired) {
                                preloadEvent(EVT_CACHE_REBALANCE_STOPPED, exchFut.discoveryEvent());

                                stopEvtFired = true;
                            }
                        }
                    }
                    catch (BrokenBarrierException ignore) {
                        throw new InterruptedException("Demand worker stopped.");
                    }

                    // Sync up all demand threads at this step.
                    GridDhtPreloaderAssignments assigns = null;

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

                                GridDhtPartitionDemandMessage d = assigns.remove(node);

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
                                        log.debug("Node left during rebalancing (will retry) [node=" + node.id() +
                                            ", msg=" + e.getMessage() + ']');

                                    resync = true;

                                    break; // For.
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to receive partitions from node (rebalancing will not " +
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
    void updateLastExchangeFuture(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /**
     * @param exchFut Exchange future.
     * @return Assignments of partitions to nodes.
     */
    GridDhtPreloaderAssignments assign(GridDhtPartitionsExchangeFuture exchFut) {
        // No assignments for disabled preloader.
        GridDhtPartitionTopology top = cctx.dht().topology();

        if (!cctx.rebalanceEnabled())
            return new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        int partCnt = cctx.affinity().partitions();

        assert exchFut.forcePreload() || exchFut.dummyReassign() ||
            exchFut.exchangeId().topologyVersion().equals(top.topologyVersion()) :
            "Topology version mismatch [exchId=" + exchFut.exchangeId() +
                ", topVer=" + top.topologyVersion() + ']';

        GridDhtPreloaderAssignments assigns = new GridDhtPreloaderAssignments(exchFut, top.topologyVersion());

        AffinityTopologyVersion topVer = assigns.topologyVersion();

        for (int p = 0; p < partCnt; p++) {
            if (cctx.shared().exchange().hasPendingExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping assignments creation, exchange worker has pending assignments: " +
                        exchFut.exchangeId());

                break;
            }

            // If partition belongs to local node.
            if (cctx.affinity().localNode(p, topVer)) {
                GridDhtLocalPartition part = top.localPartition(p, topVer, true);

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

                    if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                        DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                        cctx.events().addPreloadEvent(p,
                            EVT_CACHE_REBALANCE_PART_DATA_LOST, discoEvt.eventNode(),
                            discoEvt.type(), discoEvt.timestamp());
                    }

                    if (log.isDebugEnabled())
                        log.debug("Owning partition as there are no other owners: " + part);
                }
                else {
                    ClusterNode n = F.first(picked);

                    GridDhtPartitionDemandMessage msg = assigns.get(n);

                    if (msg == null) {
                        assigns.put(n, msg = new GridDhtPartitionDemandMessage(
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
            assert workers.size() == poolSize();

            remaining = Collections.synchronizedList(new LinkedList<>(workers));
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
    private static class SupplyMessage {
        /** Sender ID. */
        private UUID sndId;

        /** Supply message. */
        private GridDhtPartitionSupplyMessage supply;

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
        SupplyMessage(UUID sndId, GridDhtPartitionSupplyMessage supply) {
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
        GridDhtPartitionSupplyMessage supply() {
            return supply;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SupplyMessage.class, this);
        }
    }
}