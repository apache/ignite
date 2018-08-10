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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.getLong;

/**
 * Class that serves asynchronous part eviction process.
 * Only one partition from group can be evicted at the moment.
 */
public class PartitionsEvictManager extends GridCacheSharedManagerAdapter {
    /** Lock object. */
    private final Object mux = new Object();

    /** Default eviction progress show frequency. */
    private static final int DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS = 2 * 60 * 1000; // 2 Minutes.

    /** Eviction progress frequency property name. */
    private static final String SHOW_EVICTION_PROGRESS_FREQ = "SHOW_EVICTION_PROGRESS_FREQ";

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs = getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** Next time of show eviction progress. */
    private long nextShowProgressTime;

    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /** Flag indicates that eviction process has stopped. */
    private volatile boolean stop;

    /** */
    private final EvictionContext sharedEvictionContext = () -> stop;

    /** */
    private static final int THREADS = 4;

    /** How many eviction task may execute concurrent. */
    private int permits = THREADS;

    /** Eviction thread pool policy. */
    private static final byte EVICT_POOL_PLC = GridIoPolicy.SYSTEM_POOL;

    private final BucketQueue evictionQueue = new BucketQueue(THREADS);

    /**
     * Stops eviction process.
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
     * Adds partition to eviction queue and starts eviction process.
     * @param grp Gr
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        if (stop)
            return;

        int groupId = grp.groupId();

        GroupEvictionContext groupEvictionContext = evictionGroupsMap.get(groupId);

        if (groupEvictionContext == null) {
            groupEvictionContext = new GroupEvictionContext(grp);

            GroupEvictionContext prev = evictionGroupsMap.putIfAbsent(groupId, groupEvictionContext);

            if (prev != null)
                groupEvictionContext = prev;
        }

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
        if (stop)
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

                    // Submit task to executor.
                    IgniteInternalFuture<?> fut = cctx
                        .kernalContext()
                        .closure()
                        .runLocalSafe(evictionTask, EVICT_POOL_PLC);

                    fut.listen(f -> {
                        synchronized (mux) {
                            // Return permit after task completed.
                            permits++;
                        }

                        // Re-schedule new one task form same bucket.
                        scheduleNextPartitionEviction(bucket);
                    });

                    // Register task future, may need if group or node will be stopped.
                    groupEvictionContext.taskScheduled(fut);
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
                    ", threads=" + THREADS +
                    ", groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + "]");

            evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

            nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        stop = false;
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
        private final List<IgniteInternalFuture<?>> partsEvictFutures = new ArrayList<>();

        /** Flag indicates that eviction process has stopped for this group. */
        private volatile boolean stop;

        /** Total partition to evict. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** Total partition evict in progress. */
        private int taskInProgress;

        private GroupEvictionContext(CacheGroupContext grp) {
            this.grp = grp;
        }

        /** {@inheritDoc} */
        @Override public boolean shouldStop() {
            return stop || sharedEvictionContext.shouldStop();
        }

        private PartitionEvictionTask createEvictPartitionTask(GridDhtLocalPartition part){
            if (shouldStop() || !partIds.add(part.id()))
                return null;

            totalTasks.incrementAndGet();

            return new PartitionEvictionTask(part, this);
        }

        private synchronized void taskScheduled(IgniteInternalFuture<?> fut) {
            if (shouldStop())
                return;

            taskInProgress++;

            partsEvictFutures.add(fut);

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    if (totalTasks.decrementAndGet() == 0)
                        evictionGroupsMap.remove(grp.groupId());
                }
            });
        }

        private void stop() {
            stop = true;
        }

        private synchronized void awaitFinishAll(){
            partsEvictFutures.forEach(this::awaitFinish);

            evictionGroupsMap.remove(grp.groupId());
        }

        private synchronized void awaitFinish(IgniteInternalFuture<?> fut){
            // Wait for last offered partition eviction completion
            try {
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
            if (groupEvictionContext.shouldStop())
                return;

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
               // groupEvictionContext.evictionFut.onDone();
            }
            catch (Throwable ex) {
               // groupEvictionContext.evictionFut.onDone(ex);

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

    private class BucketQueue {
        /** Queues contains partitions scheduled for eviction. */
        private final Queue<PartitionEvictionTask>[] buckets;

        private final long[] bucketSizes;

        BucketQueue(int buckets) {
            this.buckets = new Queue[buckets];

            for (int i = 0; i < buckets; i++)
                this.buckets[i] = createEvictPartitionQueue();

            bucketSizes = new long[buckets];
        }

        PartitionEvictionTask poll(int bucket) {
            PartitionEvictionTask task = buckets[bucket].poll();

            bucketSizes[bucket] -= task.size;

            return task;
        }

        PartitionEvictionTask pollAny() {
            for (int bucket = 0; bucket < bucketSizes.length; bucket++){
                if (bucketSizes.length > 0)
                    return poll(bucket);
            }

            return null;
        }

        int offer(PartitionEvictionTask task) {
            int bucket = calculateBucket(task);

            buckets[bucket].offer(task);

            bucketSizes[bucket] += task.size;

            return bucket;
        }

        boolean isEmpty(){
            return size() == 0;
        }

        int size(){
            int size = 0;

            for (Queue<PartitionEvictionTask> queue : buckets) {
                size += queue.size();
            }

            return size;
        }

        private int calculateBucket(PartitionEvictionTask task) {
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
        private static final byte QUEUE_TYPE = 0;

        /**
         *
         * @return Queue for evict partitions.
         */
        private Queue<PartitionEvictionTask> createEvictPartitionQueue() {
            switch (QUEUE_TYPE) {
                case 1:
                    return new PriorityBlockingQueue<>(
                        1000,
                        (p1, p2) -> p1.part.fullSize() > p2.part.fullSize() ? -1 : 1);
                default:
                    return new LinkedBlockingQueue<>();
            }
        }
    }
}
