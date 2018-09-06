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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVICTION_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getLong;

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
    private final long evictionProgressFreqMs = getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** */
    private final int confPermits = getInteger(IGNITE_EVICTION_PERMITS, -1);

    /** Next time of show eviction progress. */
    private long nextShowProgressTime;

    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /** Flag indicates that eviction process has stopped. */
    private volatile boolean stop;

    /** Check stop eviction context. */
    private final EvictionContext sharedEvictionContext = () -> stop;

    /** Number of maximum concurrent operations. */
    private volatile int threads;

    /** How many eviction task may execute concurrent. */
    private volatile int permits;

    /** Bucket queue for load balance partitions to the threads via count of partition size.
     *  Is not thread-safe.
     *  All method should be called under mux synchronization.
     */
    private volatile BucketQueue evictionQueue;

    /** Lock object. */
    private final Object mux = new Object();

    /**
     * Stops eviction process for group.
     *
     * Method awaits last offered partition eviction.
     *
     * @param grp Group context.
     */
    public void onCacheGroupStopped(CacheGroupContext  grp){
        GroupEvictionContext groupEvictionContext = evictionGroupsMap.remove(grp.groupId());

        if (groupEvictionContext != null){
            groupEvictionContext.stop();

            groupEvictionContext.awaitFinishAll();
        }
    }

    /**
     * Adds partition to eviction queue and starts eviction process if permit available.
     *
     * @param grp Group context.
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        // Check node stop.
        if (sharedEvictionContext.shouldStop())
            return;

        GroupEvictionContext groupEvictionContext = evictionGroupsMap.computeIfAbsent(
            grp.groupId(), (k) -> new GroupEvictionContext(grp));

        PartitionEvictionTask evictionTask = groupEvictionContext.createEvictPartitionTask(part);

        if (evictionTask == null)
            return;

        int bucket;

        synchronized (mux) {
            bucket = evictionQueue.offer(evictionTask);
        }

        scheduleNextPartitionEviction(bucket);
    }

    /**
     * Gets next partition from the queue and schedules it for eviction.
     *
     * @param bucket Bucket.
     */
    private void scheduleNextPartitionEviction(int bucket) {
        // Check node stop.
        if (sharedEvictionContext.shouldStop())
            return;

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

                    GroupEvictionContext groupEvictionContext = evictionTask.groupEvictionContext;

                    // Check that group or node stopping.
                    if (groupEvictionContext.shouldStop())
                        continue;

                    // Get permit for this task.
                    permits--;

                    // Register task future, may need if group or node will be stopped.
                    groupEvictionContext.taskScheduled(evictionTask);

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
        if (U.currentTimeMillis() >= nextShowProgressTime) {
            int size = evictionQueue.size() + 1; // Queue size plus current partition.

            if (log.isInfoEnabled())
                log.info("Eviction in progress [permits=" + permits+
                    ", threads=" + threads +
                    ", groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + "]");

            evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

            nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;
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

        log.info("Evict partition permits=" + permits);

        evictionQueue = new BucketQueue(threads);
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        super.stop0(cancel);

        stop = true;

        Collection<GroupEvictionContext> evictionGrps = evictionGroupsMap.values();

        evictionGrps.forEach(GroupEvictionContext::stop);

        evictionGrps.forEach(GroupEvictionContext::awaitFinishAll);
    }

    /**
     *
     */
    private class GroupEvictionContext implements EvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Deduplicate set partition ids. */
        private final Set<Integer> partIds = new GridConcurrentHashSet<>();

        /** Future for currently running partition eviction task. */
        private final Map<Integer, IgniteInternalFuture<?>> partsEvictFutures = new ConcurrentHashMap<>();

        /** Flag indicates that eviction process has stopped for this group. */
        private volatile boolean stop;

        /** Total partition to evict. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** Total partition evict in progress. */
        private int taskInProgress;

        /**
         * @param grp Group context.
         */
        private GroupEvictionContext(CacheGroupContext grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public boolean shouldStop() {
            return stop || sharedEvictionContext.shouldStop();
        }

        /**
         *
         * @param part Grid local partition.
         */
        private PartitionEvictionTask createEvictPartitionTask(GridDhtLocalPartition part){
            if (shouldStop() || !partIds.add(part.id()))
                return null;

            totalTasks.incrementAndGet();

            return new PartitionEvictionTask(part, this);
        }

        /**
         *
         * @param task Partition eviction task.
         */
        private synchronized void taskScheduled(PartitionEvictionTask task) {
            if (shouldStop())
                return;

            taskInProgress++;

            GridFutureAdapter<?> fut = task.finishFut;

            int partId = task.part.id();

            partsEvictFutures.put(partId, fut);

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    partsEvictFutures.remove(partId, f);

                    if (totalTasks.decrementAndGet() == 0)
                        evictionGroupsMap.remove(grp.groupId());
                }
            });
        }

        /**
         * Stop eviction for group.
         */
        private void stop() {
            stop = true;
        }

        /**
         * Await evict finish.
         */
        private void awaitFinishAll(){
            partsEvictFutures.forEach(this::awaitFinish);

            evictionGroupsMap.remove(grp.groupId());
        }

        /**
         * Await evict finish partition.
         */
        private void awaitFinish(Integer part, IgniteInternalFuture<?> fut) {
            // Wait for last offered partition eviction completion
            try {
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
                log.info("Group eviction in progress [grpName=" + grp.cacheOrGroupName()+
                    ", grpId=" + grp.groupId() +
                    ", remainingPartsToEvict=" + (totalTasks.get() - taskInProgress) +
                    ", partsEvictInProgress=" + taskInProgress +
                    ", totalParts= " + grp.topology().localPartitions().size() + "]");
        }
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    private class PartitionEvictionTask implements Runnable {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        private final long size;

        /** Eviction context. */
        private final GroupEvictionContext groupEvictionContext;

        /** */
        private final GridFutureAdapter<?> finishFut = new GridFutureAdapter<>();

        /**
         * @param part Partition.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext groupEvictionContext
        ) {
            this.part = part;
            this.groupEvictionContext = groupEvictionContext;

            size = part.fullSize();
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (groupEvictionContext.shouldStop()) {
                finishFut.onDone();

                return;
            }

            try {
                boolean success = part.tryClear(groupEvictionContext);

                if (success) {
                    if (part.state() == GridDhtPartitionState.EVICTED && part.markForDestroy())
                        part.destroy();
                }
                else // Re-offer partition if clear was unsuccessful due to partition reservation.
                    evictionQueue.offer(this);

                // Complete eviction future before schedule new to prevent deadlock with
                // simultaneous eviction stopping and scheduling new eviction.
                finishFut.onDone();
            }
            catch (Throwable ex) {
                finishFut.onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction failed (current node is stopping).",
                        false,
                        true);
                }
                else{
                    LT.error(log, ex, "Partition eviction failed, this can cause grid hang.");
                }
            }
        }
    }

    /**
     *
     */
    private class BucketQueue {
        /** Queues contains partitions scheduled for eviction. */
        private final Queue<PartitionEvictionTask>[] buckets;

        /** */
        private final long[] bucketSizes;

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
            for (int bucket = 0; bucket < bucketSizes.length; bucket++){
                if (!buckets[bucket].isEmpty())
                    return poll(bucket);
            }

            return null;
        }

        /**
         * Offer task to queue.
         *
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
        boolean isEmpty(){
            return size() == 0;
        }

        /**
         * @return Queue size.
         */
        int size(){
            int size = 0;

            for (Queue<PartitionEvictionTask> queue : buckets) {
                size += queue.size();
            }

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
         * 0 - PRIORITY QUEUE (compare by partition size).
         * default (any other values) - FIFO.
         */
        private static final byte QUEUE_TYPE = 1;

        /**
         *
         * @return Queue for evict partitions.
         */
        private Queue<PartitionEvictionTask> createEvictPartitionQueue() {
            switch (QUEUE_TYPE) {
                case 1:
                    return new PriorityBlockingQueue<>(
                        1000, Comparator.comparingLong(p -> p.part.fullSize()));
                default:
                    return new LinkedBlockingQueue<>();
            }
        }
    }
}
