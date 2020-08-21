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
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheStoppedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.util.Objects.nonNull;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVICTION_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;

/**
 * Class that serves asynchronous part eviction process.
 * Multiple partition from group can be evicted at the same time.
 */
public class PartitionsEvictManager extends GridCacheSharedManagerAdapter {
    /** Default eviction progress show frequency. */
    private static final int DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS = 2 * 60 * 1000; // 2 Minutes.

    /** Eviction progress frequency property name. */
    private static final String SHOW_EVICTION_PROGRESS_FREQ = "SHOW_EVICTION_PROGRESS_FREQ";

    /** Eviction thread pool policy. */
    private static final byte EVICT_POOL_PLC = GridIoPolicy.SYSTEM_POOL;

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs =
        getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** */
    private final int confPermits = getInteger(IGNITE_EVICTION_PERMITS, -1);

    /** Last time of show eviction progress. */
    private long lastShowProgressTimeNanos = System.nanoTime() - U.millisToNanos(evictionProgressFreqMs);

    /** */
    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /**
     * Evicted partitions for printing to log. It works under {@link #mux}.
     */
    private final Map<Integer, Map<Integer, EvictReason>> logEvictPartByGrps = new HashMap<>();

    /** Number of maximum concurrent operations. */
    private volatile int threads;

    /** How many eviction task may execute concurrent. */
    private volatile int permits;

    /**
     * Bucket queue for load balance partitions to the threads via count of
     * partition size. Is not thread-safe. All method should be called under
     * mux synchronization.
     */
    volatile BucketQueue evictionQueue;

    /** Lock object. */
    private final Object mux = new Object();

    /** */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /**
     * Callback on cache group start.
     *
     * @param grp group.
     */
    public void onCacheGroupStarted(CacheGroupContext grp) {
        evictionGroupsMap.remove(grp.groupId());
    }

    /**
     * Stops eviction process for group.
     *
     * Method awaits last offered partition eviction.
     *
     * @param grp Group context.
     */
    public void onCacheGroupStopped(CacheGroupContext grp) {
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
     * @param reason Reason for eviction.
     */
    public IgniteInternalFuture<?> evictPartitionAsync(
        CacheGroupContext grp,
        GridDhtLocalPartition part,
        EvictReason reason
    ) {
        assert nonNull(grp);
        assert nonNull(part);
        assert nonNull(reason);

        if (!busyLock.readLock().tryLock())
            return new GridFinishedFuture<>(new NodeStoppingException("Node is stopping"));

        try {
            int grpId = grp.groupId();

            GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
                grpId, (k) -> new GroupEvictionContext(grp));

            int bucket;

            synchronized (mux) {
                int partId = part.id();

                boolean added = grpEvictionCtx.partIds.add(partId);

                assert added : "The partition is already clearing " + part;

                PartitionEvictionTask task = new PartitionEvictionTask(part, grpEvictionCtx, reason);

                bucket = evictionQueue.offer(task);

                logEvictPartByGrps.computeIfAbsent(grpId, i -> new HashMap<>()).put(partId, reason);

                grpEvictionCtx.totalTasks.incrementAndGet();

                if (log.isDebugEnabled())
                    log.debug("Partition has been scheduled for eviction [grp=" + grp.cacheOrGroupName()
                        + ", p=" + part.id() + ", state=" + part.state() + "]");

                scheduleNextPartitionEviction(bucket);

                return task.finishFut;
            }
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Gets next partition from the queue and schedules it for eviction.
     *
     * @param bucket Bucket.
     */
    private void scheduleNextPartitionEviction(int bucket) {
        synchronized (mux) {
            // Check that we have permits for next operation.
            if (permits > 0) {
                // If queue is empty not need to do.
                if (evictionQueue.isEmpty())
                    return;

                // Get task until we have permits.
                while (permits >= 0) {
                    // Get task from bucket.
                    PartitionEvictionTask evictionTask = evictionQueue.poll(bucket);

                    // If bucket empty try get from another.
                    if (evictionTask == null) {
                        // Until queue have tasks.
                        while (!evictionQueue.isEmpty()) {
                            // Get task from any other bucket.
                            evictionTask = evictionQueue.pollAny();

                            // Stop iteration if we found task.
                            if (evictionTask != null)
                                break;
                        }

                        // If task not found no need to do some.
                        if (evictionTask == null)
                            return;
                    }

                    // Print current eviction progress.
                    showProgress();

                    GroupEvictionContext grpEvictionCtx = evictionTask.grpEvictionCtx;

                    // Get permit for this task.
                    permits--;

                    // Register task future, may need if group or node will be stopped.
                    grpEvictionCtx.taskScheduled(evictionTask);

                    evictionTask.finishFut.listen(f -> {
                        synchronized (mux) {
                            // Return permit after task completed.
                            permits++;
                        }

                        // Re-schedule new one task form same bucket.
                        scheduleNextPartitionEviction(bucket);
                    });

                    // Submit task to executor.
                    cctx.kernalContext()
                        .closure()
                        .runLocalSafe(evictionTask, EVICT_POOL_PLC);
                }
            }
        }
    }

    /**
     * Shows progress of eviction.
     */
    private void showProgress() {
        if (U.millisSinceNanos(lastShowProgressTimeNanos) >= evictionProgressFreqMs) {
            int size = evictionQueue.size() + 1; // Queue size plus current partition.

            if (log.isInfoEnabled()) {
                log.info("Eviction in progress [permits=" + permits +
                    ", threads=" + threads +
                    ", groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + "]");

                evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

                if (!logEvictPartByGrps.isEmpty()) {
                    StringJoiner evictPartJoiner = new StringJoiner(", ");

                    logEvictPartByGrps.forEach((grpId, evictParts) -> {
                        CacheGroupContext grpCtx = cctx.cache().cacheGroup(grpId);
                        String grpName = (nonNull(grpCtx) ? grpCtx.cacheOrGroupName() : null);

                        String partByReasonStr = partByReasonStr(evictParts);

                        evictPartJoiner.add("[grpId=" + grpId + ", grpName=" + grpName + ", " + partByReasonStr + ']');
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

        // If property is not setup, calculate permits as parts of sys pool.
        if (confPermits == -1) {
            int sysPoolSize = cctx.kernalContext().config().getSystemThreadPoolSize();

            threads = permits = sysPoolSize / 4;
        }
        else
            threads = permits = confPermits;

        // Avoid 0 permits if sys pool size less that 4.
        if (threads == 0)
            threads = permits = 1;

        if (log.isInfoEnabled())
            log.info("Evict partition permits=" + permits);

        evictionQueue = new BucketQueue(threads);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        // Prevents new eviction tasks from appearing.
        busyLock.writeLock().lock();

        Collection<GroupEvictionContext> evictionGrps = evictionGroupsMap.values();

        NodeStoppingException ex = new NodeStoppingException("Node is stopping");

        // Ignore cancel flag for group eviction because it may take a while.
        for (GroupEvictionContext evictionGrp : evictionGrps)
            evictionGrp.stop(ex);
    }

    /**
     * Creating a group partitions for reasons of eviction as a string.
     *
     * @param evictParts Partitions with a reason for eviction.
     * @return String with group partitions for reasons of eviction.
     */
    private String partByReasonStr(Map<Integer, EvictReason> evictParts) {
        assert nonNull(evictParts);

        Map<EvictReason, Collection<Integer>> partByReason = new EnumMap<>(EvictReason.class);

        for (Entry<Integer, EvictReason> entry : evictParts.entrySet())
            partByReason.computeIfAbsent(entry.getValue(), b -> new ArrayList<>()).add(entry.getKey());

        StringJoiner joiner = new StringJoiner(", ");

        partByReason.forEach((reason, partIds) -> joiner.add(reason.toString() + '=' + S.compact(partIds)));

        return joiner.toString();
    }

    /**
     *
     */
    private class GroupEvictionContext implements EvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Deduplicate set partition ids. */
        private final Set<Integer> partIds = new HashSet<>();

        /** Future for currently running partition eviction task. */
        private final Map<Integer, IgniteInternalFuture<?>> partsEvictFutures = new ConcurrentHashMap<>();

        /** Stop exception. */
        private AtomicReference<Exception> stopExRef = new AtomicReference<>();

        /** Total partition to evict. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** Total partition evict in progress. */
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

            int partId = task.part.id();

            partIds.remove(partId);

            partsEvictFutures.put(partId, fut);

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    partsEvictFutures.remove(partId, f);

                    totalTasks.decrementAndGet();
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
         * Await evict finish.
         */
        private void awaitFinishAll() {
            partsEvictFutures.forEach(this::awaitFinish);
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
     * Awaits all futures.
     */
    public void awaitFinishAll() {
        evictionGroupsMap.values().forEach(GroupEvictionContext::awaitFinishAll);
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    class PartitionEvictionTask implements Runnable {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        /** */
        private final long size;

        /** Reason for eviction. */
        private final EvictReason reason;

        /** Eviction context. */
        private final GroupEvictionContext grpEvictionCtx;

        /** */
        private final GridFutureAdapter<?> finishFut = new GridFutureAdapter<>();

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         * @param reason Reason for eviction.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx,
            EvictReason reason
        ) {
            this.part = part;
            this.grpEvictionCtx = grpEvictionCtx;
            this.reason = reason;

            size = part.fullSize();
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
                        "part=" + part.id() + ", cleared=" + clearedEntities + ']');
                }

                finishFut.onDone();
            }
            catch (Throwable ex) {
                finishFut.onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction has been cancelled (local node is stopping) " +
                        "[grp=" + grpEvictionCtx.grp.cacheOrGroupName() +
                        ", readyVer=" + grpEvictionCtx.grp.topology().readyTopologyVersion() + ']',
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
     *
     */
    class BucketQueue {
        /** */
        private final long[] bucketSizes;

        /** Queues contains partitions scheduled for eviction. */
        final Queue<PartitionEvictionTask>[] buckets;

        /**
         * @param buckets Number of buckets.
         */
        BucketQueue(int buckets) {
            this.buckets = new Queue[buckets];

            for (int i = 0; i < buckets; i++)
                this.buckets[i] = createEvictPartitionQueue();

            bucketSizes = new long[buckets];
        }

        /**
         * Poll eviction task from queue for specific bucket.
         *
         * @param bucket Bucket index.
         * @return Partition evict task, or {@code null} if bucket queue is empty.
         */
        PartitionEvictionTask poll(int bucket) {
            PartitionEvictionTask task = buckets[bucket].poll();

            if (task != null)
                bucketSizes[bucket] -= task.size;

            return task;
        }

        /**
         * Poll eviction task from queue (bucket is not specific).
         *
         * @return Partition evict task.
         */
        PartitionEvictionTask pollAny() {
            for (int bucket = 0; bucket < bucketSizes.length; bucket++) {
                if (!buckets[bucket].isEmpty())
                    return poll(bucket);
            }

            return null;
        }

        /**
         * Offer task to queue.
         *
         * @param task Eviction task.
         * @return Bucket index.
         */
        int offer(PartitionEvictionTask task) {
            int bucket = calculateBucket();

            buckets[bucket].offer(task);

            bucketSizes[bucket] += task.size;

            return bucket;
        }


        /**
         * @return {@code True} if queue is empty, {@code} False if not empty.
         */
        boolean isEmpty() {
            return size() == 0;
        }

        /**
         * @return Queue size.
         */
        int size() {
            int size = 0;

            for (Queue<PartitionEvictionTask> queue : buckets)
                size += queue.size();

            return size;
        }

        /***
         * @return Bucket index.
         */
        private int calculateBucket() {
            int min = 0;

            for (int bucket = min; bucket < bucketSizes.length; bucket++) {
                if (bucketSizes[min] > bucketSizes[bucket])
                    min = bucket;
            }

            return min;
        }

        /**
         *
         * @return Queue for evict partitions, prioritized by size.
         */
        private Queue<PartitionEvictionTask> createEvictPartitionQueue() {
            return new PriorityBlockingQueue<>(
                1000, Comparator.comparingLong(p -> p.part.fullSize()));
        }
    }

    /**
     * Reason for eviction of partition.
     */
    enum EvictReason {
        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#RENTING RENTING} state.
         */
        EVICTION,

        /**
         * Partition evicted after changing to
         * {@link GridDhtPartitionState#MOVING MOVING} state.
         */
        CLEARING;

        /** {@inheritDoc} */
        @Override public String toString() {
            return name().toLowerCase();
        }
    }
}
