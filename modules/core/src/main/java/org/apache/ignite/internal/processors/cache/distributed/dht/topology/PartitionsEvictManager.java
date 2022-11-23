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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.SystemProperty;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Class that serves asynchronous part eviction process.
 * Multiple partition from group can be evicted at the same time.
 */
public class PartitionsEvictManager extends GridCacheSharedManagerAdapter {
    /** Default eviction progress show frequency. */
    private static final int DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS = 2 * 60 * 1000;

    /** Eviction progress frequency property name. */
    @SystemProperty(value = "Eviction progress frequency in milliseconds", type = Long.class,
        defaults = "" + DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS)
    public static final String SHOW_EVICTION_PROGRESS_FREQ = "SHOW_EVICTION_PROGRESS_FREQ";

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs =
        getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** Last time of show eviction progress. */
    private long lastShowProgressTimeNanos = System.nanoTime() - U.millisToNanos(evictionProgressFreqMs);

    /** */
    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /**
     * Evicted partitions for printing to log. Should be updated holding a lock on {@link #mux}.
     */
    private final Map<Integer, Map<Integer, EvictReason>> logEvictPartByGrps = new HashMap<>();

    /** */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** Lock object. */
    private final Object mux = new Object();

    /** The executor for clearing jobs. */
    private volatile IgniteThreadPoolExecutor executor;

    /**
     * Callback on cache group start.
     *
     * @param grp Group.
     */
    public void onCacheGroupStarted(CacheGroupContext grp) {
        // No-op.
    }

    /**
     * Stops eviction process for group.
     *
     * Method awaits last offered partition eviction.
     *
     * @param grp Group context.
     */
    public void onCacheGroupStopped(CacheGroupContext grp) {
        // Must keep context in the map to avoid race with subsequent clearing request after the call to this method.
        GroupEvictionContext grpEvictionCtx =
            evictionGroupsMap.computeIfAbsent(grp.groupId(), p -> new GroupEvictionContext(grp));

        grpEvictionCtx.stop(new CacheStoppedException(grp.cacheOrGroupName()));
    }

    /**
     * Adds partition to eviction queue and starts eviction process if permit
     * available.
     *
     * @param grp Group context.
     * @param part Partition to evict.
     * @param finishFut Clearing finish future.
     */
    public IgniteInternalFuture<?> evictPartitionAsync(
        CacheGroupContext grp,
        GridDhtLocalPartition part,
        GridFutureAdapter<?> finishFut
    ) {
        assert nonNull(grp);
        assert nonNull(part);

        if (!busyLock.readLock().tryLock())
            return new GridFinishedFuture<>(new NodeStoppingException("Node is stopping"));

        try {
            int grpId = grp.groupId();

            if (cctx.cache().cacheGroup(grpId) == null)
                return new GridFinishedFuture<>(new CacheStoppedException(grp.cacheOrGroupName()));

            GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
                grpId, k -> new GroupEvictionContext(grp));

            EvictReason reason = context().kernalContext().recoveryMode() ? EvictReason.CLEARING_ON_RECOVERY :
                part.state() == RENTING ? EvictReason.EVICTION : EvictReason.CLEARING;

            if (log.isDebugEnabled())
                log.debug("The partition has been scheduled for clearing [grp=" + grp.cacheOrGroupName()
                    + ", topVer=" + (cctx.kernalContext().recoveryMode() ?
                    AffinityTopologyVersion.NONE : grp.topology().readyTopologyVersion())
                    + ", id=" + part.id() + ", state=" + part.state()
                    + ", fullSize=" + part.fullSize() + ", reason=" + reason + ']');

            synchronized (mux) {
                PartitionEvictionTask task = new PartitionEvictionTask(part, grpEvictionCtx, reason, finishFut);

                logEvictPartByGrps.computeIfAbsent(grpId, i -> new HashMap<>()).put(part.id(), reason);

                grpEvictionCtx.totalTasks.incrementAndGet();

                updateMetrics(grp, reason, INCREMENT);

                executor.submit(task);

                showProgress();

                grpEvictionCtx.taskScheduled(task);

                return task.finishFut;
            }
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Shows progress of eviction.
     */
    private void showProgress() {
        if (U.millisSinceNanos(lastShowProgressTimeNanos) >= evictionProgressFreqMs) {
            int size = executor.getQueue().size();

            if (log.isInfoEnabled()) {
                log.info("Eviction in progress [groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + ']');

                evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

                if (!logEvictPartByGrps.isEmpty()) {
                    StringJoiner evictPartJoiner = new StringJoiner(", ");

                    logEvictPartByGrps.forEach((grpId, map) -> {
                        CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpId);

                        String grpName = (nonNull(grpCtx) ? grpCtx.cacheOrGroupName() : null);

                        evictPartJoiner.add("[grpId=" + grpId + ", grpName=" + grpName + ", " + toString(map) + ']');
                    });

                    log.info("Partitions have been scheduled for eviction: " + evictPartJoiner);

                    logEvictPartByGrps.clear();
                }
            }

            lastShowProgressTimeNanos = System.nanoTime();
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        executor = (IgniteThreadPoolExecutor)cctx.kernalContext().pools().getRebalanceExecutorService();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override protected void onKernalStop0(boolean cancel) {
        super.onKernalStop0(cancel);

        // Prevents new eviction tasks from appearing.
        busyLock.writeLock().lock();

        Collection<GroupEvictionContext> evictionGrps = evictionGroupsMap.values();

        NodeStoppingException ex = new NodeStoppingException("Node is stopping");

        // Ignore cancel flag for group eviction because it may take a while.
        for (GroupEvictionContext evictionGrp : evictionGrps)
            evictionGrp.stop(ex);

        executor = null;
    }

    /**
     * Creating a group partitions for reasons of eviction as a string.
     *
     * @param evictParts Partitions with a reason for eviction.
     * @return String with group partitions for reasons of eviction.
     */
    private String toString(Map<Integer, EvictReason> evictParts) {
        assert nonNull(evictParts);

        Map<EvictReason, Collection<Integer>> partByReason = new EnumMap<>(EvictReason.class);

        for (Entry<Integer, EvictReason> entry : evictParts.entrySet())
            partByReason.computeIfAbsent(entry.getValue(), b -> new ArrayList<>()).add(entry.getKey());

        StringJoiner joiner = new StringJoiner(", ");

        partByReason.forEach((reason, partIds) -> joiner.add(reason.toString() + '=' + S.toStringSortedDistinct(partIds)));

        return joiner.toString();
    }

    /**
     * Cleans up group eviction context when it's safe.
     *
     * @param grpId Group id.
     */
    public void cleanupRemovedGroup(int grpId) {
        evictionGroupsMap.remove(grpId);
    }

    /**
     *
     */
    private class GroupEvictionContext implements EvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Stop exception. */
        private AtomicReference<Exception> stopExRef = new AtomicReference<>();

        /** Total partition to evict. Can be replaced by the metric counters. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** Total partition evicts in progress. */
        private int taskInProgress;

        /** */
        private ReadWriteLock busyLock = new ReentrantReadWriteLock();

        /**
         * @param grp Group context.
         */
        private GroupEvictionContext(CacheGroupContext grp) {
            this.grp = grp;
        }

        /**
         *
         * @param task Partition eviction task.
         */
        private synchronized void taskScheduled(PartitionEvictionTask task) {
            taskInProgress++;

            GridFutureAdapter<?> fut = task.finishFut;

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    totalTasks.decrementAndGet();

                    updateMetrics(task.grpEvictionCtx.grp, task.reason, DECREMENT);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public boolean shouldStop() {
            return stopExRef.get() != null;
        }

        /**
         * @param ex Stop exception.
         */
        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        void stop(Exception ex) {
            // Prevent concurrent stop.
            if (!stopExRef.compareAndSet(null, ex))
                return;

            busyLock.writeLock().lock();
        }

        /**
         * Await evict finish partition.
         */
        private void awaitFinish(Integer part, IgniteInternalFuture<?> fut) {
            // Wait for last offered partition eviction completion
            try {
                if (log.isInfoEnabled())
                    log.info("Await partition evict, grpName=" + grp.cacheOrGroupName() +
                        ", grpId=" + grp.groupId() + ", partId=" + part);

                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.warning("Failed to await partition eviction during stopping.", e);
            }
        }

        /**
         * Shows progress group of eviction.
         */
        private void showProgress() {
            if (log.isInfoEnabled())
                log.info("Group eviction in progress [grpName=" + grp.cacheOrGroupName() +
                    ", grpId=" + grp.groupId() +
                    ", remainingPartsToEvict=" + (totalTasks.get() - taskInProgress) +
                    ", partsEvictInProgress=" + taskInProgress +
                    ", totalParts=" + grp.topology().localPartitions().size() + "]");
        }
    }

    /**
     * @return The number of executing + waiting in the queue tasks.
     */
    public int total() {
        return evictionGroupsMap.values().stream().mapToInt(ctx -> ctx.totalTasks.get()).sum();
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    private class PartitionEvictionTask implements Runnable {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        /** Reason for eviction. */
        private final EvictReason reason;

        /** Eviction context. */
        private final GroupEvictionContext grpEvictionCtx;

        /** */
        private final GridFutureAdapter<?> finishFut;

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         * @param reason Reason for eviction.
         * @param finishFut Finish future.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx,
            EvictReason reason,
            GridFutureAdapter<?> finishFut
        ) {
            this.part = part;
            this.grpEvictionCtx = grpEvictionCtx;
            this.reason = reason;
            this.finishFut = finishFut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (!grpEvictionCtx.busyLock.readLock().tryLock()) {
                finishFut.onDone(grpEvictionCtx.stopExRef.get());

                return;
            }

            try {
                long clearedEntities = part.clearAll(grpEvictionCtx);

                if (log.isDebugEnabled()) {
                    log.debug("The partition has been cleared [grp=" + part.group().cacheOrGroupName() +
                        ", topVer=" + (cctx.kernalContext().recoveryMode() ?
                        AffinityTopologyVersion.NONE : part.group().topology().readyTopologyVersion()) +
                        ", id=" + part.id() + ", state=" + part.state() + ", cleared=" + clearedEntities +
                        ", fullSize=" + part.fullSize() + ']');
                }

                finishFut.onDone();
            }
            catch (Throwable ex) {
                updateMetrics(grpEvictionCtx.grp, reason, DECREMENT);

                finishFut.onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction has been cancelled (local node is stopping) " +
                        "[grp=" + grpEvictionCtx.grp.cacheOrGroupName() +
                        ", readyVer=" + (cctx.kernalContext().recoveryMode() ?
                            AffinityTopologyVersion.NONE : grpEvictionCtx.grp.topology().readyTopologyVersion()) +
                        ']',
                        false,
                        true);
                }
                else {
                    LT.error(log, ex, "Partition eviction has failed [grp=" +
                        grpEvictionCtx.grp.cacheOrGroupName() + ", part=" + part.id() + ']');

                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, ex));
                }
            }
            finally {
                grpEvictionCtx.busyLock.readLock().unlock();
            }
        }
    }

    /**
     * Reason for eviction of partition.
     */
    private enum EvictReason {
        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#RENTING RENTING} state.
         */
        EVICTION,

        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#MOVING MOVING} state.
         */
        CLEARING,

        /**
         * Partition clearing on logical WAL recovery.
         * Used to repeat partition clearing if the node was stopped without previous clearing checkpointed.
         */
        CLEARING_ON_RECOVERY;

        /** {@inheritDoc} */
        @Override public String toString() {
            return name().toLowerCase();
        }
    }

    /**
     * @param grp Cache group.
     * @param c Update closure.
     */
    private void updateMetrics(CacheGroupContext grp, EvictReason reason, BiConsumer<EvictReason, CacheMetricsImpl> c) {
        if (reason != EvictReason.CLEARING_ON_RECOVERY) {
            for (GridCacheContext cctx : grp.caches()) {
                if (cctx.statisticsEnabled()) {
                    final CacheMetricsImpl metrics = cctx.cache().metrics0();

                    c.accept(reason, metrics);
                }
            }
        }
    }

    /** Increment closure. */
    private static final BiConsumer<EvictReason, CacheMetricsImpl> INCREMENT = new BiConsumer<EvictReason, CacheMetricsImpl>() {
        @Override public void accept(EvictReason reason, CacheMetricsImpl cacheMetrics) {
            if (reason == EvictReason.CLEARING)
                cacheMetrics.incrementRebalanceClearingPartitions();
            else
                cacheMetrics.incrementEvictingPartitions();
        }
    };

    /** Decrement closure. */
    private static final BiConsumer<EvictReason, CacheMetricsImpl> DECREMENT = new BiConsumer<EvictReason, CacheMetricsImpl>() {
        @Override public void accept(EvictReason reason, CacheMetricsImpl cacheMetrics) {
            if (reason == EvictReason.CLEARING)
                cacheMetrics.decrementRebalanceClearingPartitions();
            else
                cacheMetrics.decrementEvictingPartitions();
        }
    };
}
