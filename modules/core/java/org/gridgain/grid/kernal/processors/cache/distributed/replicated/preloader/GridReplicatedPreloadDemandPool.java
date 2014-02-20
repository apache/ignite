// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;

/**
 * Thread pool for demanding entries.
 *
 * @author @java.author
 * @version @java.version
 */
class GridReplicatedPreloadDemandPool<K, V> {
    /** Dummy message to wake up demand worker. */
    private static final SupplyMessage DUMMY_MSG = new SupplyMessage();

    /** Cache context. */
    private final GridCacheContext<K, V> cctx;

    /** Logger. */
    private final GridLogger log;

    /** Busy lock. */
    private final ReadWriteLock busyLock;

    /** Demand workers. */
    private final Collection<DemandWorker> workers = new LinkedList<>();

    /** Left assignments. */
    private final AtomicInteger leftAssigns = new AtomicInteger();

    /** Max order of the nodes to preload from. */
    private final GridAtomicLong maxOrder = new GridAtomicLong();

    /** Assignments. */
    private final BlockingQueue<GridReplicatedPreloadAssignment> assigns =
        new LinkedBlockingQueue<>();

    /** Timeout. */
    private final AtomicLong timeout = new AtomicLong();

    /** Lock to prevent preloading for the time of eviction. */
    private final ReadWriteLock evictLock;

    /** Demand lock for undeploys. */
    private ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /** Future to get done after assignments completion. */
    private GridFutureAdapter<?> finishFut;

    /** Predicate to check whether preloading is permitted. */
    private GridPredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /** Node comparator. */
    private Comparator<GridNode> nodeCmp = new Comparator<GridNode>() {
        @Override public int compare(GridNode n1, GridNode n2) {
            return n1.order() < n2.order() ? -1 : n1.order() > n2.order() ? 1 : 0;
        }
    };

    /**
     * @param cctx Cache context.
     * @param busyLock Shutdown lock.
     * @param evictLock Preloading lock.
     * @param preloadPred Predicate to check whether preloading is permitted.
     */
    GridReplicatedPreloadDemandPool(GridCacheContext<K, V> cctx, ReadWriteLock busyLock, ReadWriteLock evictLock,
        GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        assert cctx != null;
        assert busyLock != null;
        assert evictLock != null;
        assert preloadPred != null;

        this.cctx = cctx;
        this.busyLock = busyLock;
        this.evictLock = evictLock;
        this.preloadPred = preloadPred;

        log = cctx.logger(getClass());

        int poolSize = cctx.preloadEnabled() ? cctx.config().getPreloadThreadPoolSize() : 1;

        timeout.set(cctx.config().getPreloadTimeout());

        for (int i = 0; i < poolSize; i++)
            workers.add(new DemandWorker(i));
    }

    /**
     *
     */
    void start() {
        for (DemandWorker w : workers)
            new GridThread(cctx.gridName(), "preloader-demand-worker", w).start();

        if (log.isDebugEnabled())
            log.debug("Started demand pool: " + workers.size());
    }

    /**
     * Sets preload predicate to replicated demand pool.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        this.preloadPred = F.and(this.preloadPred, preloadPred);
    }

    /**
     * @param assigns Assignments collection.
     * @param finishFut Future to get done after assignments completion.
     * @param maxOrder Max order of the nodes to preload from.
     */
    void assign(Collection<GridReplicatedPreloadAssignment> assigns, GridFutureAdapter<?> finishFut, long maxOrder) {
        assert !F.isEmpty(assigns);
        assert finishFut != null;
        assert maxOrder > 0;

        leftAssigns.set(assigns.size());

        this.maxOrder.set(maxOrder);

        this.finishFut = finishFut;

        this.assigns.addAll(assigns);
    }

    /**
     *
     */
    private void onAssignmentProcessed() {
        if (leftAssigns.decrementAndGet() == 0) {
            boolean b = finishFut.onDone();

            assert b;
        }
    }

    /**
     *
     */
    void stop() {
        U.cancel(workers);
        U.join(workers, log);
    }

    /**
     * @return {@code true} if entered busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (demand pool is stopping): " + cctx.nodeId());

        return false;
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
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param queue Queue to poll from.
     * @param time Time to wait.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> queue, long time, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return queue.poll(time, MILLISECONDS);
    }

    /**
     * Demand worker.
     */
    private class DemandWorker extends GridWorker {
        /** Worker ID. */
        private int id = -1;

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
         * @param msg Message.
         */
        private void addMessage(SupplyMessage<K, V> msg) {
            if (!enterBusy())
                return;

            try {
                assert msg == DUMMY_MSG || msg.message().workerId() == id : "Invalid message: " + msg;

                msgQ.offer(msg);
            }
            finally {
                leaveBusy();
            }
        }

        /**
         * @param timeout Timeout value.
         */
        private void growTimeout(long timeout) {
            long newTimeout = (long)(timeout * 1.5D);

            // Account for overflow.
            if (newTimeout < 0)
                newTimeout = Long.MAX_VALUE;

            // Grow by 50% only if another thread didn't do it already.
            if (GridReplicatedPreloadDemandPool.this.timeout.compareAndSet(timeout, newTimeout))
                U.warn(log, "Increased preloading message timeout from " + timeout + "ms to " +
                    newTimeout + "ms.");
        }

        /**
         * @param idx Unique index for this topic.
         * @return Topic for partition.
         */
        private Object topic(long idx) {
            return TOPIC_CACHE.topic(cctx.namexx(), cctx.nodeId(), id, idx);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
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
                catch (GridException e) {
                    U.error(log, "Failed to wait for ordered preload future [cacheName=" + cctx.name() +
                        ", preloadOrder=" + preloadOrder + ']', e);
                }
            }

            while (!isCancelled()) {
                GridReplicatedPreloadAssignment assign = assigns.poll(cctx.gridConfig().getNetworkTimeout(),
                    MILLISECONDS);

                if (assign == null) {
                    // Block preloading for undeploys.
                    if (demandLock.writeLock().tryLock()) {
                        try {
                            cctx.deploy().unwind();
                        }
                        finally {
                            demandLock.writeLock().unlock();
                        }
                    }

                    continue;
                }

                demandLock.readLock().lock();

                try {
                    processAssignment(assign);
                }
                finally {
                    demandLock.readLock().unlock();
                }

                onAssignmentProcessed();
            }
        }

        /**
         * @param assign Assignment.
         * @throws GridInterruptedException If thread is interrupted.
         * @throws InterruptedException If thread is interrupted.
         */
        private void processAssignment(GridReplicatedPreloadAssignment assign) throws GridInterruptedException,
            InterruptedException {
            assert assign != null;

            assert cctx.preloadEnabled();

            if (log.isDebugEnabled())
                log.debug("Processing assignment: " + assign);

            while (!isCancelled()) {
                long topVer = maxOrder.get();

                if (topVer < 1) {
                    if (log.isDebugEnabled())
                        log.debug("Cannot complete assignment (all elder nodes have not finished preloading yet): " +
                            assign);

                    return;
                }

                List<GridNode> nodes = new ArrayList<>();

                // Filter cached affinity nodes.
                for (GridNode n : cctx.affinity().nodes(assign.partition(), topVer)) {
                    if (cctx.discovery().alive(n.id()))
                        nodes.add(n);
                }

                if (nodes.isEmpty()) {
                    if (log.isDebugEnabled())
                        log.debug("Cannot complete assignment (all elder nodes left): " + assign);

                    return;
                }

                Collections.sort(nodes, nodeCmp);

                GridNode node = nodes.get(assign.mod() % nodes.size());

                try {
                    if (!demandFromNode(node, assign))
                        continue; // Retry to complete assignment with next node.

                    break; // Assignment has been processed.
                }
                catch (GridInterruptedException e) {
                    throw e;
                }
                catch (GridTopologyException e) {
                    if (log.isDebugEnabled())
                        log.debug("Node left during preloading (will retry) [node=" + node.id() +
                            ", msg=" + e.getMessage() + ']');
                }
                catch (GridException e) {
                    U.error(log, "Failed to receive entries from node (will retry): " + node.id(), e);
                }
            }
        }

        /**
         * @param node Node to demand from.
         * @param assign Assignment.
         * @return {@code True} if assignment has been fully processed.
         * @throws InterruptedException If thread is interrupted.
         * @throws GridException If failed.
         */
        private boolean demandFromNode(final GridNode node, GridReplicatedPreloadAssignment assign)
            throws InterruptedException, GridException {

            // Drain queue before processing a new node.
            drainQueue();

            GridLocalEventListener discoLsnr = new GridLocalEventListener() {
                @SuppressWarnings({"unchecked"})
                @Override public void onEvent(GridEvent evt) {
                    assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                    if (node.id().equals(((GridDiscoveryEvent)evt).eventNodeId()))
                        addMessage(DUMMY_MSG);
                }
            };

            cctx.events().addListener(discoLsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);

            cntr++;

            GridReplicatedPreloadDemandMessage<K, V> d = new GridReplicatedPreloadDemandMessage<>(
                assign.partition(), assign.mod(), assign.nodeCount(), topic(cntr), timeout.get(), id);

            if (isCancelled())
                return true; // Pool is being stopped.

            cctx.io().addOrderedHandler(d.topic(), new CI2<UUID, GridReplicatedPreloadSupplyMessage<K, V>>() {
                @Override public void apply(UUID nodeId, GridReplicatedPreloadSupplyMessage<K, V> msg) {
                    addMessage(new SupplyMessage<>(nodeId, msg));
                }
            });

            try {
                boolean retry;

                boolean stopOnDummy = false;

                // DoWhile.
                // =======
                do {
                    retry = false;

                    long timeout = GridReplicatedPreloadDemandPool.this.timeout.get();

                    d.timeout(timeout);

                    // Send demand message.
                    cctx.io().send(node, d);

                    if (log.isDebugEnabled())
                        log.debug("Sent demand message [node=" + node.id() + ", msg=" + d + ']');

                    // While.
                    // =====
                    while (!isCancelled()) {
                        SupplyMessage<K, V> s = poll(msgQ, timeout, this);

                        // If timed out.
                        if (s == null) {
                            if (msgQ.isEmpty()) { // Safety check.
                                U.warn(log, "Timed out waiting for preload response from node " +
                                    "(you may need to increase 'networkTimeout' or 'preloadBatchSize' " +
                                    "configuration properties): " + node.id());

                                growTimeout(timeout);

                                // Ordered listener was removed if timeout expired.
                                cctx.io().removeOrderedHandler(d.topic());

                                // Must create copy to be able to work with IO manager thread local caches.
                                d = new GridReplicatedPreloadDemandMessage<>(d);

                                // Create new topic.
                                d.topic(topic(++cntr));

                                // Create new ordered listener.
                                cctx.io().addOrderedHandler(d.topic(),
                                    new CI2<UUID, GridReplicatedPreloadSupplyMessage<K, V>>() {
                                        @Override public void apply(UUID nodeId,
                                            GridReplicatedPreloadSupplyMessage<K, V> msg) {
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
                        else if (s == DUMMY_MSG) {
                            if (!stopOnDummy) {
                                // Possibly event came prior to rest of messages from node.
                                stopOnDummy = true;

                                // Add dummy message to queue again.
                                addMessage(s);
                            }
                            else
                                // Quit preloading.
                                break;

                            continue;
                        }

                        // Check that message was received from expected node.
                        if (!s.senderId().equals(node.id())) {
                            U.warn(log, "Received supply message from unexpected node [expectedId=" + node.id() +
                                ", rcvdId=" + s.senderId() + ", msg=" + s + ']');

                            continue; // While.
                        }

                        if (log.isDebugEnabled())
                            log.debug("Received supply message: " + s);

                        GridReplicatedPreloadSupplyMessage<K, V> supply = s.message();

                        if (supply.failed()) {
                            // Node is preloading now and therefore cannot supply.
                            maxOrder.setIfLess(node.order() - 1); // Preload from nodes elder, than node.

                            // Quit preloading.
                            break;
                        }

                        // Check whether there were class loading errors on unmarshalling.
                        if (supply.classError() != null) {
                            if (log.isDebugEnabled())
                                log.debug("Class got undeployed during preloading: " + supply.classError());

                            retry = true;

                            // Retry preloading.
                            break;
                        }

                        preload(supply);

                        if (supply.last())
                            // Assignment is finished.
                            return true;
                    }
                }
                while (retry && !isCancelled());
            }
            finally {
                cctx.io().removeOrderedHandler(d.topic());

                cctx.events().removeListener(discoLsnr);
            }

            return false;
        }

        /**
         * @param supply Supply message.
         */
        private void preload(GridReplicatedPreloadSupplyMessage<K, V> supply) {
            boolean rec = cctx.events().isRecordable(EVT_CACHE_PRELOAD_OBJECT_LOADED);

            evictLock.readLock().lock(); // Prevent evictions.

            try {
                for (GridCacheEntryInfo<K, V> info : supply.entries()) {
                    if (!preloadPred.apply(info)) {
                        if (log.isDebugEnabled())
                            log.debug("Preloading is not permitted for entry: " + info);

                        continue;
                    }

                    GridCacheEntryEx<K, V> cached = null;

                    try {
                        cached = cctx.cache().entryEx(info.key());

                        if (cctx.cache().isGgfsDataCache() &&
                            cctx.cache().ggfsDataSpaceUsed() > cctx.cache().ggfsDataSpaceMax()) {
                            LT.error(log, null, "Failed to preload GGFS data cache (GGFS space size exceeded maximum " +
                                "value, will ignore preload entries): " + name());

                            if (cached.markObsoleteIfEmpty(null))
                                cached.context().cache().removeIfObsolete(cached.key());

                            continue;
                        }

                        if (cached.initialValue(
                            info.value(),
                            info.valueBytes(),
                            info.version(),
                            info.ttl(),
                            info.expireTime(),
                            true,
                            DR_NONE
                        )) {
                            cctx.evicts().touch(cached); // Start tracking.

                            if (rec && !cached.isInternal())
                                cctx.events().addEvent(cached.partition(), cached.key(), cctx.localNodeId(),
                                    (GridUuid)null, null, EVT_CACHE_PRELOAD_OBJECT_LOADED, info.value(), true, null,
                                    false);
                        }
                        else if (log.isDebugEnabled())
                            log.debug("Preloading entry is already in cache (will ignore): " + cached);
                    }
                    catch (GridCacheEntryRemovedException ignored) {
                        if (log.isDebugEnabled())
                            log.debug("Entry has been concurrently removed while preloading: " + cached);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to put preloaded entry.", e);
                    }
                }
            }
            finally {
                evictLock.readLock().unlock(); // Let evicts run.
            }
        }

        /**
         * @throws InterruptedException If interrupted.
         */
        private void drainQueue() throws InterruptedException {
            while (msgQ.peek() != null) {
                SupplyMessage<K, V> msg = msgQ.take();

                if (log.isDebugEnabled())
                    log.debug("Drained supply message: " + msg);
            }
        }
    }

    /**
     * Supply message wrapper.
     */
    private static class SupplyMessage<K, V> extends GridBiTuple<UUID, GridReplicatedPreloadSupplyMessage<K, V>> {
        /**
         * @param sndId Sender ID.
         * @param msg Message.
         */
        SupplyMessage(UUID sndId, GridReplicatedPreloadSupplyMessage<K, V> msg) {
            super(sndId, msg);
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public SupplyMessage() {
            // No-op.
        }

        /**
         * @return Sender ID.
         */
        UUID senderId() {
            return get1();
        }

        /**
         * @return Message.
         */
        public GridReplicatedPreloadSupplyMessage<K, V> message() {
            return get2();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "SupplyMessage [senderId=" + senderId() + ", msg=" + message() + ']';
        }
    }
}
