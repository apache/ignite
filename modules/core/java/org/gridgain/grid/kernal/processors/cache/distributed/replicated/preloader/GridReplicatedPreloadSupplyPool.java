// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.worker.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Thread pool for supplying entries to demanding nodes.
 *
 * @author @java.author
 * @version @java.version
 */
class GridReplicatedPreloadSupplyPool<K, V> {
    /** Cache context. */
    private final GridCacheContext<K, V> cctx;

    /** Logger. */
    private final GridLogger log;

    /** Predicate to check whether local node has already finished preloading. */
    private final GridAbsPredicate preloadFinished;

    /** Busy lock. */
    private final ReadWriteLock busyLock;

    /** Workers. */
    private final Collection<SupplyWorker> workers = new LinkedList<>();

    /** Queue. */
    private final BlockingQueue<DemandMessage<K, V>> queue = new LinkedBlockingQueue<>();

    /** */
    private final boolean depEnabled;

    /** Preload predicate. */
    private GridPredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /**
     * @param cctx Cache context.
     * @param preloadFinished Preload finished callback.
     * @param busyLock Shutdown lock.
     */
    GridReplicatedPreloadSupplyPool(GridCacheContext<K, V> cctx, GridAbsPredicate preloadFinished,
        ReadWriteLock busyLock) {
        assert cctx != null;
        assert preloadFinished != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.preloadFinished = preloadFinished;
        this.busyLock = busyLock;

        log = cctx.logger(getClass());

        int poolSize = cctx.preloadEnabled() ? cctx.config().getPreloadThreadPoolSize() : 0;

        for (int i = 0; i < poolSize; i++)
            workers.add(new SupplyWorker());

        cctx.io().addHandler(GridReplicatedPreloadDemandMessage.class,
            new CI2<UUID, GridReplicatedPreloadDemandMessage<K, V>>() {
                @Override public void apply(UUID id, GridReplicatedPreloadDemandMessage<K, V> m) {
                    addMessage(id, m);
                }
            });

        depEnabled = cctx.gridDeploy().enabled();
    }

    /**
     *
     */
    void start() {
        for (SupplyWorker w : workers)
            new GridThread(cctx.gridName(), "preloader-supply-worker", w).start();

        if (log.isDebugEnabled())
            log.debug("Started supply pool: " + workers.size());
    }

    /**
     *
     */
    void stop() {
        U.cancel(workers);
        U.join(workers, log);
    }

    /**
     * Sets preload predicate.
     *
     * @param preloadPred Preload predicate.
     */
    void preloadPredicate(GridPredicate<GridCacheEntryInfo<K, V>> preloadPred) {
        this.preloadPred = preloadPred;
    }

    /**
     * @return {@code true} if entered busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (supplier is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param nodeId Sender node ID.
     * @param d Message.
     */
    private void addMessage(UUID nodeId, GridReplicatedPreloadDemandMessage<K, V> d) {
        if (!enterBusy())
            return;

        try {
            if (cctx.preloadEnabled()) {
                if (log.isDebugEnabled())
                    log.debug("Received demand message [node=" + nodeId + ", msg=" + d + ']');

                queue.offer(new DemandMessage<>(nodeId, d));
            }
            else
                U.warn(log, "Received demand message when preloading is disabled (will ignore): " + d);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     * @param queue Deque to poll from.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    private <T> T take(BlockingQueue<T> queue, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return queue.take();
    }

    /**
     * Supply worker.
     */
    private class SupplyWorker extends GridWorker {
        /**
         * Default constructor.
         */
        private SupplyWorker() {
            super(cctx.gridName(), "preloader-supply-worker", log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, GridInterruptedException {
            long preloadThrottle = cctx.config().getPreloadThrottle();

            while (!isCancelled()) {
                DemandMessage<K, V> msg = take(queue, this);

                GridNode node = cctx.discovery().node(msg.senderId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Received message from non-existing node (will ignore): " + msg);

                    continue;
                }

                GridReplicatedPreloadDemandMessage<K, V> d = msg.message();

                if (!preloadFinished.apply()) {
                    // Local node has not finished preloading yet and cannot supply.
                    GridReplicatedPreloadSupplyMessage<K, V> s = new GridReplicatedPreloadSupplyMessage<>(
                        d.workerId(), true);

                    try {
                        reply(node, d, s);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to send supply message to node: " + node.id(), e);
                    }

                    continue;
                }

                GridFuture<?> fut = cctx.partitionReleaseFuture(Collections.singleton(d.partition()), node.order());

                try {
                    fut.get(d.timeout());
                }
                catch (GridException e) {
                    U.error(log, "Failed to wait until partition is released: " + d.partition(), e);

                    continue;
                }

                GridReplicatedPreloadSupplyMessage<K, V> s = new GridReplicatedPreloadSupplyMessage<>(d.workerId());

                SwapListener<K, V> swapLsnr = null;

                // If demander node left grid.
                boolean nodeLeft = false;

                try {
                    boolean swapEnabled = cctx.isSwapOrOffheapEnabled();

                    if (swapEnabled) {
                        swapLsnr = new SwapListener<>();

                        cctx.swap().addOffHeapListener(d.partition(), swapLsnr);
                        cctx.swap().addSwapListener(d.partition(), swapLsnr);
                    }

                    for (GridCacheEntryEx<K, V> entry : cctx.cache().allEntries()) {
                        // Mod entry hash to the number of nodes.
                        if (U.safeAbs(entry.hashCode() % d.nodeCount()) != d.mod() ||
                            cctx.affinity().partition(entry.key()) != d.partition())
                            continue;

                        GridCacheEntryInfo<K, V> info = entry.info();

                        if (s.size() >= cctx.config().getPreloadBatchSize()) {
                            if (!reply(node, d, s)) {
                                nodeLeft = true;

                                break;
                            }

                            // Throttle preloading.
                            if (preloadThrottle > 0)
                                U.sleep(preloadThrottle);

                            s = new GridReplicatedPreloadSupplyMessage<>(d.workerId());
                        }

                        if (info != null && !(info.key() instanceof GridPartitionLockKey) && !info.isNew()) {
                            if (preloadPred == null || preloadPred.apply(info))
                                s.addEntry(info, cctx);
                            else if (log.isDebugEnabled())
                                log.debug("Preload predicate evaluated to false (will not sender cache entry): " +
                                    info);
                        }
                    }

                    if (swapEnabled) {
                        GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> iter =
                            cctx.swap().iterator(d.partition());

                        // Iterator may be null if space does not exist.
                        if (iter != null) {
                            try {
                                boolean prepared = false;

                                for (Map.Entry<byte[], GridCacheSwapEntry<V>> e : iter) {
                                    GridCacheSwapEntry<V> swapEntry = e.getValue();

                                    // Mod entry hash to the number of nodes.
                                    if (U.safeAbs(swapEntry.keyHash() % d.nodeCount()) != d.mod())
                                        continue;

                                    if (s.size() >= cctx.config().getPreloadBatchSize()) {
                                        if (!reply(node, d, s)) {
                                            nodeLeft = true;

                                            break;
                                        }

                                        // Throttle preloading.
                                        if (preloadThrottle > 0)
                                            U.sleep(preloadThrottle);

                                        s = new GridReplicatedPreloadSupplyMessage<>(d.workerId());
                                    }

                                    GridCacheEntryInfo<K, V> info = new GridCacheEntryInfo<>();

                                    info.keyBytes(e.getKey());
                                    info.valueBytes(swapEntry.valueBytes());
                                    info.ttl(swapEntry.ttl());
                                    info.expireTime(swapEntry.expireTime());
                                    info.version(swapEntry.version());

                                    if (preloadPred == null || preloadPred.apply(info))
                                        s.addEntry0(info, cctx);
                                    else {
                                        if (log.isDebugEnabled())
                                            log.debug("Preload predicate evaluated to false (will not send " +
                                                "cache entry): " + info);

                                        continue;
                                    }

                                    // Need to manually prepare cache message.
                                    if (depEnabled && !prepared) {
                                        ClassLoader ldr = swapEntry.keyClassLoaderId() != null ?
                                            cctx.deploy().getClassLoader(swapEntry.keyClassLoaderId()) :
                                            swapEntry.valueClassLoaderId() != null ?
                                                cctx.deploy().getClassLoader(swapEntry.valueClassLoaderId()) : null;

                                        if (ldr == null)
                                            continue;

                                        if (ldr instanceof GridDeploymentInfo) {
                                            s.prepare((GridDeploymentInfo)ldr);

                                            prepared = true;
                                        }
                                    }
                                }
                            }
                            finally {
                                iter.close();
                            }
                        }

                        // Stop receiving promote notifications.
                        cctx.swap().removeSwapListener(d.partition(), swapLsnr);

                        Collection<GridCacheEntryInfo<K, V>> entries = swapLsnr.entries();

                        swapLsnr = null;

                        for (GridCacheEntryInfo<K, V> info : entries) {
                            if (s.size() >= cctx.config().getPreloadBatchSize()) {
                                if (!reply(node, d, s)) {
                                    nodeLeft = true;

                                    break;
                                }

                                s = new GridReplicatedPreloadSupplyMessage<>(d.workerId());
                            }

                            if (preloadPred == null || preloadPred.apply(info))
                                s.addEntry(info, cctx);
                            else {
                                if (log.isDebugEnabled())
                                    log.debug("Preload predicate evaluated to false (will not sender cache entry): " +
                                        info);
                            }
                        }
                    }

                    // Do that only if node has not left yet.
                    if (!nodeLeft) {
                        // Cache entries are fully iterated at this point.
                        s.last(true);

                        reply(node, d, s);
                    }
                }
                catch (GridException e) {
                    U.error(log, "Failed to send supply message to node: " + node.id(), e);

                    // Removing current topic because of request must fail with timeout and
                    // demander will generate new topic.
                    cctx.io().removeMessageId(d.topic());
                }
                finally {
                    if (swapLsnr != null)
                        cctx.swap().removeSwapListener(d.partition(), swapLsnr);

                    if (s.last() || nodeLeft) {
                        cctx.io().removeMessageId(d.topic());
                    }
                }
            }
        }

        /**
         * @param n Node.
         * @param d Demand message.
         * @param s Supply message.
         * @return {@code True} if message was sent, {@code false} if recipient left grid.
         * @throws GridException If failed.
         */
        private boolean reply(GridNode n, GridReplicatedPreloadDemandMessage<K, V> d,
            GridReplicatedPreloadSupplyMessage<K, V> s) throws GridException {
            try {
                cctx.io().sendOrderedMessage(n, d.topic(), cctx.io().messageId(d.topic(), n.id()), s, d.timeout());

                if (log.isDebugEnabled())
                    log.debug("Replied to demand message [node=" + n.id() + ", demand=" + d + ", supply=" + s + ']');

                return true;
            }
            catch (GridTopologyException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send supply message because node left grid: " + n.id());

                return false;
            }
        }
    }

    /**
     * Demand message wrapper.
     */
    private static class DemandMessage<K, V> extends GridBiTuple<UUID, GridReplicatedPreloadDemandMessage<K, V>> {
        /**
         * @param sndId Sender ID.
         * @param msg Message.
         */
        DemandMessage(UUID sndId, GridReplicatedPreloadDemandMessage<K, V> msg) {
            super(sndId, msg);
        }

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public DemandMessage() {
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
        public GridReplicatedPreloadDemandMessage<K, V> message() {
            return get2();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "DemandMessage [senderId=" + senderId() + ", msg=" + message() + ']';
        }
    }

    /**
     *
     */
    private class SwapListener<K, V> implements GridCacheSwapListener<K, V> {
        /** */
        private final Map<K, GridCacheEntryInfo<K, V>> swappedEntries =
            new ConcurrentHashMap8<>();

        /** {@inheritDoc} */
        @Override public void onEntryUnswapped(int part, K key, byte[] keyBytes,
            V val, byte[] valBytes, GridCacheVersion ver, long ttl, long expireTime) {
            if (log.isDebugEnabled())
                log.debug("Received unswapped event for key: " + key);

            GridCacheEntryInfo<K, V> info = new GridCacheEntryInfo<>();

            info.keyBytes(keyBytes);
            info.value(val);
            info.valueBytes(valBytes);
            info.ttl(ttl);
            info.expireTime(expireTime);
            info.version(ver);

            if (info != null && info.value() != null)
                swappedEntries.put(key, info);
        }

        /**
         * @return Entries, received by listener.
         */
        Collection<GridCacheEntryInfo<K, V>> entries() {
            return swappedEntries.values();
        }
    }
}
