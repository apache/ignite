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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Thread pool for supplying partitions to demanding nodes.
 */
class GridDhtPartitionSupplyPool<K, V> {
    /** */
    private final GridCacheContext<K, V> cctx;

    /** */
    private final IgniteLogger log;

    /** */
    private final ReadWriteLock busyLock;

    /** */
    private GridDhtPartitionTopology<K, V> top;

    /** */
    private final Collection<SupplyWorker> workers = new LinkedList<>();

    /** */
    private final BlockingQueue<DemandMessage<K, V>> queue = new LinkedBlockingDeque<>();

    /** */
    private final boolean depEnabled;

    /** Preload predicate. */
    private IgnitePredicate<GridCacheEntryInfo<K, V>> preloadPred;

    /**
     * @param cctx Cache context.
     * @param busyLock Shutdown lock.
     */
    GridDhtPartitionSupplyPool(GridCacheContext<K, V> cctx, ReadWriteLock busyLock) {
        assert cctx != null;
        assert busyLock != null;

        this.cctx = cctx;
        this.busyLock = busyLock;

        log = cctx.logger(getClass());

        top = cctx.dht().topology();

        int poolSize = cctx.preloadEnabled() ? cctx.config().getPreloadThreadPoolSize() : 0;

        for (int i = 0; i < poolSize; i++)
            workers.add(new SupplyWorker());

        cctx.io().addHandler(cctx.cacheId(), GridDhtPartitionDemandMessage.class, new CI2<UUID, GridDhtPartitionDemandMessage<K, V>>() {
            @Override public void apply(UUID id, GridDhtPartitionDemandMessage<K, V> m) {
                processDemandMessage(id, m);
            }
        });

        depEnabled = cctx.gridDeploy().enabled();
    }

    /**
     *
     */
    void start() {
        for (SupplyWorker w : workers)
            new IgniteThread(cctx.gridName(), "preloader-supply-worker", w).start();
    }

    /**
     *
     */
    void stop() {
        U.cancel(workers);
        U.join(workers, log);

        top = null;
    }

    /**
     * Sets preload predicate for supply pool.
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
        return cctx.config().getPreloadThreadPoolSize();
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    private boolean enterBusy() {
        if (busyLock.readLock().tryLock())
            return true;

        if (log.isDebugEnabled())
            log.debug("Failed to enter to busy state (supplier is stopping): " + cctx.nodeId());

        return false;
    }

    /**
     * @param nodeId Sender node ID.
     * @param d Message.
     */
    private void processDemandMessage(UUID nodeId, GridDhtPartitionDemandMessage<K, V> d) {
        if (!enterBusy())
            return;

        try {
            if (cctx.preloadEnabled()) {
                if (log.isDebugEnabled())
                    log.debug("Received partition demand [node=" + nodeId + ", demand=" + d + ']');

                queue.offer(new DemandMessage<>(nodeId, d));
            }
            else
                U.warn(log, "Received partition demand message when preloading is disabled (will ignore): " + d);
        }
        finally {
            leaveBusy();
        }
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * @param deque Deque to poll from.
     * @param w Worker.
     * @return Polled item.
     * @throws InterruptedException If interrupted.
     */
    @Nullable private <T> T poll(BlockingQueue<T> deque, GridWorker w) throws InterruptedException {
        assert w != null;

        // There is currently a case where {@code interrupted}
        // flag on a thread gets flipped during stop which causes the pool to hang.  This check
        // will always make sure that interrupted flag gets reset before going into wait conditions.
        // The true fix should actually make sure that interrupted flag does not get reset or that
        // interrupted exception gets propagated. Until we find a real fix, this method should
        // always work to make sure that there is no hanging during stop.
        if (w.isCancelled())
            Thread.currentThread().interrupt();

        return deque.poll(2000, MILLISECONDS);
    }

    /**
     * Supply work.
     */
    private class SupplyWorker extends GridWorker {
        /** Hide worker logger and use cache logger. */
        private IgniteLogger log = GridDhtPartitionSupplyPool.this.log;

        /**
         * Default constructor.
         */
        private SupplyWorker() {
            super(cctx.gridName(), "preloader-supply-worker", GridDhtPartitionSupplyPool.this.log);
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                DemandMessage<K, V> msg = poll(queue, this);

                if (msg == null)
                    continue;

                ClusterNode node = cctx.discovery().node(msg.senderId());

                if (node == null) {
                    if (log.isDebugEnabled())
                        log.debug("Received message from non-existing node (will ignore): " + msg);

                    continue;
                }

                processMessage(msg, node);
            }
        }

        /**
         * @param msg Message.
         * @param node Demander.
         */
        private void processMessage(DemandMessage<K, V> msg, ClusterNode node) {
            assert msg != null;
            assert node != null;

            GridDhtPartitionDemandMessage<K, V> d = msg.message();

            GridDhtPartitionSupplyMessage<K, V> s = new GridDhtPartitionSupplyMessage<>(d.workerId(),
                d.updateSequence(), cctx.cacheId());

            long preloadThrottle = cctx.config().getPreloadThrottle();

            boolean ack = false;

            boolean convertPortable = cctx.portableEnabled() && cctx.offheapTiered();

            try {
                // Partition map exchange is finished which means that all near transactions with given
                // topology version are committed. We can wait for local locks here as it will not take
                // much time.
                cctx.mvcc().finishLocks(d.topologyVersion()).get();

                for (Integer part : d.partitions()) {
                    GridDhtLocalPartition<K, V> loc = top.localPartition(part, d.topologyVersion(), false);

                    if (loc == null || loc.state() != OWNING || !loc.reserve()) {
                        // Reply with partition of "-1" to let sender know that
                        // this node is no longer an owner.
                        s.missed(part);

                        if (log.isDebugEnabled())
                            log.debug("Requested partition is not owned by local node [part=" + part +
                                ", demander=" + msg.senderId() + ']');

                        continue;
                    }

                    GridCacheEntryInfoCollectSwapListener<K, V> swapLsnr = null;

                    try {
                        if (cctx.isSwapOrOffheapEnabled()) {
                            swapLsnr = new GridCacheEntryInfoCollectSwapListener<>(log, cctx);

                            cctx.swap().addOffHeapListener(part, swapLsnr);
                            cctx.swap().addSwapListener(part, swapLsnr);
                        }

                        boolean partMissing = false;

                        for (GridCacheEntryEx<K, V> e : loc.entries()) {
                            if (!cctx.affinity().belongs(node, part, d.topologyVersion())) {
                                // Demander no longer needs this partition, so we send '-1' partition and move on.
                                s.missed(part);

                                if (log.isDebugEnabled())
                                    log.debug("Demanding node does not need requested partition [part=" + part +
                                        ", nodeId=" + msg.senderId() + ']');

                                partMissing = true;

                                break;
                            }

                            if (s.messageSize() >= cctx.config().getPreloadBatchSize()) {
                                ack = true;

                                if (!reply(node, d, s))
                                    return;

                                // Throttle preloading.
                                if (preloadThrottle > 0)
                                    U.sleep(preloadThrottle);

                                s = new GridDhtPartitionSupplyMessage<>(d.workerId(), d.updateSequence(),
                                    cctx.cacheId());
                            }

                            GridCacheEntryInfo<K, V> info = e.info();

                            if (info != null && !(info.key() instanceof GridPartitionLockKey) && !info.isNew()) {
                                if (preloadPred == null || preloadPred.apply(info))
                                    s.addEntry(part, info, cctx.shared());
                                else if (log.isDebugEnabled())
                                    log.debug("Preload predicate evaluated to false (will not sender cache entry): " +
                                        info);
                            }
                        }

                        if (partMissing)
                            continue;

                        if (cctx.isSwapOrOffheapEnabled()) {
                            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> iter =
                                cctx.swap().iterator(part, false);

                            // Iterator may be null if space does not exist.
                            if (iter != null) {
                                try {
                                    boolean prepared = false;

                                    for (Map.Entry<byte[], GridCacheSwapEntry<V>> e : iter) {
                                        if (!cctx.affinity().belongs(node, part, d.topologyVersion())) {
                                            // Demander no longer needs this partition,
                                            // so we send '-1' partition and move on.
                                            s.missed(part);

                                            if (log.isDebugEnabled())
                                                log.debug("Demanding node does not need requested partition " +
                                                    "[part=" + part + ", nodeId=" + msg.senderId() + ']');

                                            partMissing = true;

                                            break; // For.
                                        }

                                        if (s.messageSize() >= cctx.config().getPreloadBatchSize()) {
                                            ack = true;

                                            if (!reply(node, d, s))
                                                return;

                                            // Throttle preloading.
                                            if (preloadThrottle > 0)
                                                U.sleep(preloadThrottle);

                                            s = new GridDhtPartitionSupplyMessage<>(d.workerId(),
                                                d.updateSequence(), cctx.cacheId());
                                        }

                                        GridCacheSwapEntry<V> swapEntry = e.getValue();

                                        GridCacheEntryInfo<K, V> info = new GridCacheEntryInfo<>();

                                        info.keyBytes(e.getKey());
                                        info.ttl(swapEntry.ttl());
                                        info.expireTime(swapEntry.expireTime());
                                        info.version(swapEntry.version());

                                        if (!swapEntry.valueIsByteArray()) {
                                            if (convertPortable)
                                                info.valueBytes(cctx.convertPortableBytes(swapEntry.valueBytes()));
                                            else
                                                info.valueBytes(swapEntry.valueBytes());
                                        }
                                        else
                                            info.value(swapEntry.value());

                                        if (preloadPred == null || preloadPred.apply(info))
                                            s.addEntry0(part, info, cctx.shared());
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
                                                    cctx.deploy().getClassLoader(swapEntry.valueClassLoaderId()) :
                                                    null;

                                            if (ldr == null)
                                                continue;

                                            if (ldr instanceof GridDeploymentInfo) {
                                                s.prepare((GridDeploymentInfo)ldr);

                                                prepared = true;
                                            }
                                        }
                                    }

                                    if (partMissing)
                                        continue;
                                }
                                finally {
                                    iter.close();
                                }
                            }
                        }

                        // Stop receiving promote notifications.
                        if (swapLsnr != null) {
                            cctx.swap().removeOffHeapListener(part, swapLsnr);
                            cctx.swap().removeSwapListener(part, swapLsnr);
                        }

                        if (swapLsnr != null) {
                            Collection<GridCacheEntryInfo<K, V>> entries = swapLsnr.entries();

                            swapLsnr = null;

                            for (GridCacheEntryInfo<K, V> info : entries) {
                                if (!cctx.affinity().belongs(node, part, d.topologyVersion())) {
                                    // Demander no longer needs this partition,
                                    // so we send '-1' partition and move on.
                                    s.missed(part);

                                    if (log.isDebugEnabled())
                                        log.debug("Demanding node does not need requested partition " +
                                            "[part=" + part + ", nodeId=" + msg.senderId() + ']');

                                    // No need to continue iteration over swap entries.
                                    break;
                                }

                                if (s.messageSize() >= cctx.config().getPreloadBatchSize()) {
                                    ack = true;

                                    if (!reply(node, d, s))
                                        return;

                                    s = new GridDhtPartitionSupplyMessage<>(d.workerId(), d.updateSequence(),
                                        cctx.cacheId());
                                }

                                if (preloadPred == null || preloadPred.apply(info))
                                    s.addEntry(part, info, cctx.shared());
                                else if (log.isDebugEnabled())
                                    log.debug("Preload predicate evaluated to false (will not sender cache entry): " +
                                        info);
                            }
                        }

                        // Mark as last supply message.
                        s.last(part);

                        if (ack) {
                            s.markAck();

                            break; // Partition for loop.
                        }
                    }
                    finally {
                        loc.release();

                        if (swapLsnr != null) {
                            cctx.swap().removeOffHeapListener(part, swapLsnr);
                            cctx.swap().removeSwapListener(part, swapLsnr);
                        }
                    }
                }

                reply(node, d, s);
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Failed to send partition supply message to node: " + node.id(), e);
            }
        }

        /**
         * @param n Node.
         * @param d Demand message.
         * @param s Supply message.
         * @return {@code True} if message was sent, {@code false} if recipient left grid.
         * @throws IgniteCheckedException If failed.
         */
        private boolean reply(ClusterNode n, GridDhtPartitionDemandMessage<K, V> d, GridDhtPartitionSupplyMessage<K, V> s)
            throws IgniteCheckedException {
            try {
                if (log.isDebugEnabled())
                    log.debug("Replying to partition demand [node=" + n.id() + ", demand=" + d + ", supply=" + s + ']');

                cctx.io().sendOrderedMessage(n, d.topic(), s, cctx.ioPolicy(), d.timeout());

                return true;
            }
            catch (ClusterTopologyCheckedException ignore) {
                if (log.isDebugEnabled())
                    log.debug("Failed to send partition supply message because node left grid: " + n.id());

                return false;
            }
        }
    }

    /**
     * Demand message wrapper.
     */
    private static class DemandMessage<K, V> extends IgniteBiTuple<UUID, GridDhtPartitionDemandMessage<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param sndId Sender ID.
         * @param msg Message.
         */
        DemandMessage(UUID sndId, GridDhtPartitionDemandMessage<K, V> msg) {
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
        public GridDhtPartitionDemandMessage<K, V> message() {
            return get2();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "DemandMessage [senderId=" + senderId() + ", msg=" + message() + ']';
        }
    }
}
