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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

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
    private final long evictionProgressFreqMs = getLong(SHOW_EVICTION_PROGRESS_FREQ, DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** */
    private final int confPermits = getInteger(IGNITE_EVICTION_PERMITS, -1);

    /** Last time of show eviction progress. */
    private long lastShowProgressTimeNanos = System.nanoTime() - U.millisToNanos(evictionProgressFreqMs);

    /** */
    private final Map<Integer, GroupEvictionContext> evictionGroupsMap = new ConcurrentHashMap<>();

    /** Flag indicates that eviction process has stopped. */
    private volatile boolean stop;

    /** Check stop eviction context. */
    private final EvictionContext sharedEvictionCtx = () -> stop;

    /** Number of maximum concurrent operations. */
    private volatile int threads;

    /** How many eviction task may execute concurrent. */
    private volatile int permits;

    /** Bucket queue for load balance partitions to the threads via count of partition size.
     *  Is not thread-safe.
     *  All method should be called under mux synchronization.
     */
    volatile BucketQueue evictionQueue;

    /** Lock object. */
    private final Object mux = new Object();

    /**
     * Stops eviction process for group.
     *
     * Method awaits last offered partition eviction.
     *
     * @param grp Group context.
     */
    public void onCacheGroupStopped(CacheGroupContext grp){
        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.remove(grp.groupId());

        if (grpEvictionCtx != null){
            grpEvictionCtx.stop();

            grpEvictionCtx.awaitFinishAll();
        }
    }

    /**
     * @param grp Group context.
     * @param part Partition to clear tombstones.
     */
    public void clearTombstonesAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        if (addAsyncTask(grp, part, TaskType.CLEAR_TOMBSTONES)) {
            if (log.isDebugEnabled())
                log.debug("Partition has been scheduled for tomstones cleanup [grp=" + grp.cacheOrGroupName()
                        + ", p=" + part.id() + ", state=" + part.state() + "]");
        }
    }

    /**
     * Adds partition to eviction queue and starts eviction process if permit available.
     *
     * @param grp Group context.
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        if (addAsyncTask(grp, part, TaskType.EVICT)) {
            if (log.isDebugEnabled())
                log.debug("Partition has been scheduled for eviction [grp=" + grp.cacheOrGroupName()
                        + ", p=" + part.id() + ", state=" + part.state() + "]");
        }
    }

    /**
     * @param grp Group context.
     * @param part Partition.
     * @param type Task type.
     * @return {@code True} if task was added.
     */
    private boolean addAsyncTask(CacheGroupContext grp, GridDhtLocalPartition part, TaskType type) {
        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
            grp.groupId(), (k) -> new GroupEvictionContext(grp));

        // Check node stop.
        if (grpEvictionCtx.shouldStop())
            return false;

        int bucket;

        AbstractEvictionTask task;

        switch (type) {
            case EVICT:
                task = new PartitionEvictionTask(part, grpEvictionCtx);
                break;

            case CLEAR_TOMBSTONES:
                task = new ClearTombstonesTask(part, grpEvictionCtx);
                break;

            default:
                throw new UnsupportedOperationException("Unsupported task type: " + type);
        }

        synchronized (mux) {
            if (!grpEvictionCtx.taskIds.add(task.id))
                return false;

            bucket = evictionQueue.offer(task);
        }

        grpEvictionCtx.taskAdded(task);

        scheduleNextTask(bucket);

        return true;
    }

    /**
     * Gets next partition from the queue and schedules it for eviction.
     *
     * @param bucket Bucket.
     */
    private void scheduleNextTask(int bucket) {
        // Check node stop.
        if (sharedEvictionCtx.shouldStop())
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
                    AbstractEvictionTask evictionTask = evictionQueue.poll(bucket);

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

                    // Check that group or node stopping.
                    if (grpEvictionCtx.shouldStop())
                        continue;

                    // Get permit for this task.
                    permits--;

                    // Register task future, may need if group or node will be stopped.
                    grpEvictionCtx.taskScheduled(evictionTask);

                    evictionTask.finishFut.listen(f -> {
                        synchronized (mux) {
                            // Return permit after task completed.
                            permits++;
                        }

                        // Re-schedule new one task for same bucket.
                        scheduleNextTask(bucket);
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

            if (log.isInfoEnabled())
                log.info("Partition cleanup in progress [permits=" + permits+
                    ", threads=" + threads +
                    ", groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingTasks=" + size + "]");

            evictionGroupsMap.values().forEach(GroupEvictionContext::showProgress);

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
    private static class TasksStatistics {
        /** */
        private int total;

        /** */
        private int inProgress;

        /**
         *
         */
        void taskAdded() {
            total++;
        }

        /**
         *
         */
        void taskStarted() {
            inProgress++;
        }

        /**
         *
         */
        void taskFinished() {
            total--;
            inProgress--;
        }
    }

    /**
     *
     */
    private class GroupEvictionContext implements EvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Deduplicate set partition tasks. */
        private final Set<TaskId> taskIds = new HashSet<>();

        /** Future for currently running partition eviction task. */
        private final Map<TaskId, IgniteInternalFuture<?>> taskFutures = new ConcurrentHashMap<>();

        /** Flag indicates that eviction process has stopped for this group. */
        private volatile boolean stop;

        /** Total tasks. */
        private AtomicInteger totalTasks = new AtomicInteger();

        /** */
        private Map<TaskType, TasksStatistics> stats = U.newHashMap(TaskType.VALS.length);

        /**
         * @param grp Group context.
         */
        private GroupEvictionContext(CacheGroupContext grp) {
            this.grp = grp;

            for (TaskType type : TaskType.VALS)
                stats.put(type, new TasksStatistics());
        }

        /** {@inheritDoc} */
        @Override public boolean shouldStop() {
            return stop || sharedEvictionCtx.shouldStop();
        }

        /**
         * @param task Task.
         */
        void taskAdded(AbstractEvictionTask task) {
            totalTasks.incrementAndGet();

            synchronized (this) {
                stats.get(task.id.type).taskAdded();
            }
        }

        /**
         *
         * @param task Partition eviction task.
         */
        private synchronized void taskScheduled(AbstractEvictionTask task) {
            if (shouldStop())
                return;

            stats.get(task.id.type).taskStarted();

            GridFutureAdapter<?> fut = task.finishFut;

            taskIds.remove(task.id);

            taskFutures.put(task.id, fut);

            fut.listen(f -> {
                synchronized (this) {
                    stats.get(task.id.type).taskFinished();

                    taskFutures.remove(task.id, f);

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
            taskFutures.forEach(this::awaitFinish);

            evictionGroupsMap.remove(grp.groupId());
        }

        /**
         * Await evict finish partition.
         */
        private void awaitFinish(TaskId taskId, IgniteInternalFuture<?> fut) {
            // Wait for last offered partition eviction completion
            try {
                log.info("Await partition cleanup [grpName=" + grp.cacheOrGroupName() +
                    ", grpId=" + grp.groupId() + ", task=" + taskId.type + ", partId=" + taskId.part + ']');

                fut.get();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.warning("Failed to await partition cleanup during stopping.", e);
            }
        }

        /**
         * Shows progress group of eviction.
         */
        private void showProgress() {
            if (log.isInfoEnabled()) {
                StringBuilder msg = new StringBuilder(
                    "Group cleanup in progress [grpName=" + grp.cacheOrGroupName() + ", grpId=" + grp.groupId());

                synchronized (this) {
                    TasksStatistics evicts = stats.get(TaskType.EVICT);
                    if (evicts.total > 0) {
                        msg.append(", remainingPartsToEvict=" + (evicts.total - evicts.inProgress)).
                            append(", partsEvictInProgress=" + evicts.inProgress);
                    }

                    TasksStatistics tombstones = stats.get(TaskType.CLEAR_TOMBSTONES);
                    if (tombstones.total > 0) {
                        msg.append(", remainingPartsToClearTombstones=" + (tombstones.total - tombstones.inProgress)).
                            append(", tombstoneClearInProgress=" + tombstones.inProgress);
                    }
                }

                msg.append(", totalParts=" + grp.topology().localPartitions().size() + "]");

                log.info(msg.toString());
            }
        }
    }

    /**
     *
     */
    private enum TaskType {
        /** */
        EVICT,

        /** */
        CLEAR_TOMBSTONES;

        /** */
        private static TaskType[] VALS = values();
    }

    /**
     *
     */
    private static class TaskId {
        /** */
        final int part;

        /** */
        final TaskType type;

        /**
         * @param part Partiotion id.
         * @param type Task type.
         */
        TaskId(int part, TaskType type) {
            this.part = part;
            this.type = type;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TaskId taskKey = (TaskId)o;

            return part == taskKey.part && type == taskKey.type;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(part, type);
        }
    }

    /**
     *
     */
    abstract class AbstractEvictionTask implements Runnable {
        /** Partition to evict. */
        protected final GridDhtLocalPartition part;

        /** */
        protected final long size;

        /** Eviction context. */
        protected final GroupEvictionContext grpEvictionCtx;

        /** */
        protected final GridFutureAdapter<?> finishFut = new GridFutureAdapter<>();

        /** */
        private final TaskId id;

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         */
        private AbstractEvictionTask(
                GridDhtLocalPartition part,
                GroupEvictionContext grpEvictionCtx,
                TaskType type
        ) {
            this.part = part;
            this.grpEvictionCtx = grpEvictionCtx;

            id = new TaskId(part.id(), type);

            size = part.fullSize();
        }

        /**
         * @return {@code False} if need retry task later.
         * @throws IgniteCheckedException If failed.
         */
        abstract boolean run0() throws IgniteCheckedException;

        /**
         *
         */
        abstract void scheduleRetry();

        /** {@inheritDoc} */
        @Override public final void run() {
            if (grpEvictionCtx.shouldStop()) {
                finishFut.onDone();

                return;
            }

            try {
                boolean success = run0();

                // Complete eviction future before schedule new to prevent deadlock with
                // simultaneous eviction stopping and scheduling new eviction.
                finishFut.onDone();

                // Re-offer partition if clear was unsuccessful due to partition reservation.
                if (!success)
                    scheduleRetry();
            }
            catch (Throwable ex) {
                finishFut.onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction failed (current node is stopping).",
                        false,
                        true);
                }
                else {
                    LT.error(log, ex, "Partition eviction failed.");

                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, ex));
                }
            }
        }
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    class PartitionEvictionTask extends AbstractEvictionTask {
        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx
        ) {
            super(part, grpEvictionCtx, TaskType.EVICT);
        }

        /** {@inheritDoc} */
        @Override void scheduleRetry() {
            evictPartitionAsync(grpEvictionCtx.grp, part);
        }

        /** {@inheritDoc} */
        @Override public boolean run0() throws IgniteCheckedException {
            assert part.state() != GridDhtPartitionState.OWNING : part;

            boolean success = part.tryClear(grpEvictionCtx);

            assert part.state() != GridDhtPartitionState.OWNING : part;

            if (success) {
                if (part.state() == GridDhtPartitionState.EVICTED && part.markForDestroy())
                    part.destroy();
            }

            return success;
        }
    }

    /**
     *
     */
    class ClearTombstonesTask extends AbstractEvictionTask {
        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         */
        private ClearTombstonesTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx
        ) {
            super(part, grpEvictionCtx, TaskType.CLEAR_TOMBSTONES);
        }

        /** {@inheritDoc} */
        @Override void scheduleRetry() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean run0() throws IgniteCheckedException {
            part.clearTombstones(grpEvictionCtx);

            return true;
        }
    }

    /**
     *
     */
    class BucketQueue {
        /** Queues contains partitions scheduled for eviction. */
        final Queue<AbstractEvictionTask>[] buckets;

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
        AbstractEvictionTask poll(int bucket) {
            AbstractEvictionTask task = buckets[bucket].poll();

            if (task != null)
                bucketSizes[bucket] -= task.size;

            return task;
        }

        /**
         * Poll eviction task from queue (bucket is not specific).
         *
         * @return Partition evict task.
         */
        AbstractEvictionTask pollAny() {
            for (int bucket = 0; bucket < bucketSizes.length; bucket++){
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
        int offer(AbstractEvictionTask task) {
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

            for (Queue<AbstractEvictionTask> queue : buckets)
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
         * 0 - PRIORITY QUEUE (compare by partition size).
         * default (any other values) - FIFO.
         */
        private static final byte QUEUE_TYPE = 1;

        /**
         *
         * @return Queue for evict partitions.
         */
        private Queue<AbstractEvictionTask> createEvictPartitionQueue() {
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
