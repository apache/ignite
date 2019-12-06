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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryRowCacheCleaner;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVICTION_PERMITS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.IgniteSystemProperties.getLong;
import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_TERMINATION;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;

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
     *
     * @param grp Cache group context.
     * @param parts Partitions.
     * @throws IgniteCheckedException If failed.
     * @return Future.
     */
    public IgniteInternalFuture<Void> purgePartitionsExclusively(CacheGroupContext grp,
        List<GridDhtLocalPartition> parts) throws IgniteCheckedException {
        validateCacheGroupForExclusivePurge(grp);

        if (F.isEmpty(parts))
            return new GridFinishedFuture<>();

        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
            grp.groupId(), (k) -> new GroupEvictionContext(grp));

        ExclusivePurgeFuture fut = new ExclusivePurgeFuture(grpEvictionCtx, parts);

        synchronized (mux) {
            if (grpEvictionCtx.exclPurgeFut != null) {
                throw new IgniteCheckedException("Only one exclusive purge can be scheduled for the same cache group." +
                    " [grpId=" + grp.groupId() + "]");
            }

            Set<Integer> res = new HashSet<>();

            for (TaskId taskId : grpEvictionCtx.taskIds)
                res.add(taskId.part);

            res.retainAll(fut.partIds);

            if (!res.isEmpty()) {
                throw new IgniteCheckedException("Can't schedule exclusive purge for a partition " +
                    "due to scheduled regular clear for the same partition. [grpId=" + grp.groupId() +
                    ", partIds=" + Arrays.toString(U.toIntArray(res)) + "]");
            }

            grpEvictionCtx.exclPurgeFut = fut;

            fut.listen(f -> {
                synchronized (mux) {
                    grpEvictionCtx.exclPurgeFut = null;
                }
            });
        }

        for (GridDhtLocalPartition part : parts) {
            if (part.exclusivePurgeAsync(fut.resFut))
                fut.onInit(part);
        }

        return fut;
    }

    /**
     *
     * @param grp Group context.
     * @param part Partition to purge.
     */
    public void initExclusivePartitionPurge(CacheGroupContext grp, GridDhtLocalPartition part) {
        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.get(grp.groupId());

        if (grpEvictionCtx == null || grpEvictionCtx.shouldStop())
            return;

        synchronized (mux) {
            ExclusivePurgeFuture exclFut = grpEvictionCtx.exclPurgeFut;

            if (exclFut != null && exclFut.partIds.contains(part.id())) {
                // Exclusive purge has been requested earlier.
                if (part.exclusivePurgeAsync(exclFut.resFut))
                    exclFut.onInit(part);
            }
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

            default:
                throw new UnsupportedOperationException("Unsupported task type: " + type);
        }

        synchronized (mux) {
            ExclusivePurgeFuture exclFut = grpEvictionCtx.exclPurgeFut;

            if (exclFut != null && exclFut.partIds.contains(part.id()))
                return false;

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
     * @param grp Cache group context.
     * @throws IgniteCheckedException If failed.
     */
    private void validateCacheGroupForExclusivePurge(CacheGroupContext grp) throws IgniteCheckedException {
        if (grp.hasContinuousQueryCaches())
            throw new IgniteCheckedException("Group has active continuous queries. [grpId=" + grp.groupId() + "]");

        if (grp.shared().kernalContext().indexing().enabled())
            throw new IgniteCheckedException("IndexingSPI is enabled. [grpId=" + grp.groupId() + "]");

        for (GridCacheContext cctx : grp.caches()) {
            if (cctx.writeThrough() && !cctx.skipStore() && cctx.store().isLocal())
                throw new IgniteCheckedException("Local cache store is enabled on cache." +
                    " [grpId=" + grp.groupId() + ", cache=" + cctx.cache().name() + "]");

            if (cctx.isDrEnabled())
                throw new IgniteCheckedException("DR is enabled on cache. [grpId=" +
                    grp.groupId() + ", cache=" + cctx.cache().name() + "]");

            for (GridQueryTypeDescriptor type : grp.shared().kernalContext().query().types(cctx.name())) {
                if (type.valueTextIndex() || type.textIndex() != null)
                    throw new IgniteCheckedException("Full text index exists. [grpId=" + grp.groupId() +
                        ", cache=" + cctx.cache().name() + "]");
            }
        }
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

        /** Future for exclusive purge of a set of partitions. */
        private ExclusivePurgeFuture exclPurgeFut;

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
            ExclusivePurgeFuture exclFut;

            synchronized (mux) {
                exclFut = exclPurgeFut;

                exclPurgeFut = null;
            }

            if (exclFut != null) {
                exclFut.stop();

                awaitFinish(null, exclFut);
            }

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
        PURGE_ROWCACHE,

        /** */
        PURGE_ONHEAP_ENTRIES,

        /** */
        PURGE_INDEX;

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
        protected final GridFutureAdapter<Void> finishFut = new GridFutureAdapter<>();

        /** */
        private final TaskId id;

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         */
        private AbstractEvictionTask(
                @Nullable GridDhtLocalPartition part,
                GroupEvictionContext grpEvictionCtx,
                TaskType type
        ) {
            this.part = part;
            this.grpEvictionCtx = grpEvictionCtx;

            if (part == null) {
                id = new TaskId(INDEX_PARTITION, type);

                size = 0;
            }
            else {
                id = new TaskId(part.id(), type);

                size = part.fullSize();
            }
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
                        1000,
                        Comparator.comparing((AbstractEvictionTask t) -> t.id.type.ordinal())
                            .reversed()
                            .thenComparing((AbstractEvictionTask t) -> t.part == null ? 0 : t.part.fullSize()));
                default:
                    return new LinkedBlockingQueue<>();
            }
        }
    }

    /**
     * Context of exclusive partition purge.
     */
    private class ExclusivePurgeFuture extends GridFutureAdapter<Void> {
        /** Group eviction context. */
        GroupEvictionContext grpEvictionCtx;

        /** Partition ids. */
        Set<Integer> partIds;

        /** Partitions. */
        List<GridDhtLocalPartition> parts;

        /** All partitions initialized counter. */
        AtomicInteger initCounter;

        /** */
        AtomicInteger resultCounter;

        /** Tasks future. */
        GridCompoundFuture<Void, Void> resFut = new GridCompoundFuture<>();

        /** Allows to cancel index tasks. */
        GridCompoundFuture<Void, Void> cancelFut = new GridCompoundFuture<>();

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param parts Partitions.
         */
        public ExclusivePurgeFuture(GroupEvictionContext grpEvictionCtx, List<GridDhtLocalPartition> parts) {
            this.grpEvictionCtx = grpEvictionCtx;
            this.parts = parts;

            partIds = Collections.unmodifiableSet(
                parts.stream().mapToInt(GridDhtLocalPartition::id).boxed().collect(Collectors.toSet()));

            initCounter = new AtomicInteger(parts.size());

            resultCounter = new AtomicInteger(parts.size());
        }

        /**
         * Initializes partition for exclusive purge.
         *
         * @param part Partition.
         */
        public void onInit(GridDhtLocalPartition part) {
            if (log.isInfoEnabled())
                log.info("Partition confirmed exclusive purge. [partId=" + part.id() + "]");

            part.onClearFinished(f -> {
                if (resultCounter.decrementAndGet() == 0) {
                    assert resFut.isDone();

                    if (resFut.error() == null)
                        onDone();
                    else
                        onDone(resFut.error());
                }
            });

            if (initCounter.decrementAndGet() == 0)
                initTasks();
        }

        /**
         * Initializes tasks.
         */
        private void initTasks() {
            List<AbstractEvictionTask> tasks = new ArrayList<>();
            List<AbstractEvictionTask> idxTasks = new ArrayList<>();

            for (GridDhtLocalPartition p : parts) {
                PurgeOnheapEntriesTask task = new PurgeOnheapEntriesTask(grpEvictionCtx, p);

                tasks.add(task);

                resFut.add(task.finishFut);
            }

            CacheGroupContext grp = grpEvictionCtx.grp;

            GridCompoundFuture<Void, Void> idxFut = null;

            if (grp.shared().kernalContext().query().moduleEnabled()) {
                GridQueryIndexing idx = grp.shared().kernalContext().query().getIndexing();

                GridQueryRowCacheCleaner rowCacheCleaner = idx.rowCacheCleaner(grp.groupId());

                if (rowCacheCleaner != null) {
                    RowCachePurgeTask task = new RowCachePurgeTask(grpEvictionCtx, rowCacheCleaner, partIds);

                    tasks.add(task);

                    resFut.add(task.finishFut);
                }

                List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> idxPurgeList =
                    idx.purgeIndexPartitions(grp, partIds);

                assert idxPurgeList != null;

                if (!idxPurgeList.isEmpty()) {
                    idxFut = new GridCompoundFuture<>();

                    for (IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> r : idxPurgeList) {
                        AbstractEvictionTask idxTask = new IndexPurgeTask(grpEvictionCtx, r.get1(), r.get2());

                        cancelFut.add(r.get2());

                        idxFut.add(idxTask.finishFut);

                        idxTasks.add(idxTask);
                    }

                    cancelFut.markInitialized();

                    idxFut.markInitialized();

                    resFut.add(idxFut);
                }
            }

            resFut.markInitialized();

            if (log.isInfoEnabled())
                log.info("Exclusive partition purge: scheduling tasks." +
                    " [total=" + (tasks.size() + idxTasks.size()) + ", idx=" + idxTasks.size() + "]");

            if (idxFut == null) {
                for (AbstractEvictionTask r0 : tasks)
                    evictionQueue.offer(r0);

                grpEvictionCtx.totalTasks.addAndGet(tasks.size());
            }
            else {
                idxFut.listen(f -> {
                    if (f.error() == null && !grpEvictionCtx.shouldStop()) {
                        for (AbstractEvictionTask r0 : tasks)
                            evictionQueue.offer(r0);

                        grpEvictionCtx.totalTasks.addAndGet(tasks.size());

                        scheduleNextTask(0);
                    }
                });

                for (AbstractEvictionTask r0 : idxTasks)
                    evictionQueue.offer(r0);

                grpEvictionCtx.totalTasks.addAndGet(idxTasks.size());
            }

            scheduleNextTask(0);
        }

        /**
         * Cancels index purge tasks.
         */
        public void stop() {
            if (log.isInfoEnabled())
                log.info("Exclusive purge : stop");

            resFut.onDone(new IgniteCheckedException("Cache is stopping."));

            try {
                cancelFut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.error("Error cancelling index purge tasks", e);
            }
        }
    }

    /**
     * Purges partitions from single index.
     */
    private class IndexPurgeTask extends AbstractEvictionTask {
        /** Actual worker that removes rows from the index. */
        Runnable runnable;

        /** Future internal to the worker. */
        IgniteInternalFuture<?> fut;

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param runnable Actual worker that removes rows from the index.
         * @param fut Future internal to the worker.
         */
        IndexPurgeTask(GroupEvictionContext grpEvictionCtx, Runnable runnable, IgniteInternalFuture<?> fut) {
            super(null, grpEvictionCtx, TaskType.PURGE_INDEX);

            this.runnable = runnable;

            this.fut = fut;

            fut.listen(f -> {
                if (f.error() == null)
                    finishFut.onDone();
                else
                    finishFut.onDone(f.error());
            });
        }

        /** {@inheritDoc} */
        @Override boolean run0() {
            runnable.run();

            return true;
        }

        /** {@inheritDoc} */
        @Override void scheduleRetry() {
            // No-op.
        }
    }

    /**
     * Removes links belonging to certain partitions from H2RowCache.
     */
    private class RowCachePurgeTask extends AbstractEvictionTask {
        /** Row cache cleaner. */
        GridQueryRowCacheCleaner cleaner;

        /** Partitions. */
        Set<Integer> parts;

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param cleaner Row cache cleaner.
         * @param parts Partitions.
         */
        RowCachePurgeTask(GroupEvictionContext grpEvictionCtx, GridQueryRowCacheCleaner cleaner, Set<Integer> parts) {
            super(null, grpEvictionCtx, TaskType.PURGE_ROWCACHE);

            this.cleaner = cleaner;
            this.parts = parts;
        }

        /** {@inheritDoc} */
        @Override boolean run0() {
            cleaner.removeByPartition(parts);

            return true;
        }

        /** {@inheritDoc} */
        @Override void scheduleRetry() {
            // No-op.
        }
    }

    /**
     * Performs per-partition actions in scope of exclusive partition set purge.
     */
    private class PurgeOnheapEntriesTask extends AbstractEvictionTask {
        /** Partition. */
        private final GridDhtLocalPartition part;

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param part Partition.
         */
        PurgeOnheapEntriesTask(GroupEvictionContext grpEvictionCtx, GridDhtLocalPartition part) {
            super(part, grpEvictionCtx, TaskType.PURGE_ONHEAP_ENTRIES);

            this.part = part;
        }

        /** {@inheritDoc} */
        @Override boolean run0() throws IgniteCheckedException {
            part.cleanupRemoveQueue();

            for (GridCacheContext cctx : grpEvictionCtx.grp.caches()) {
                if (grpEvictionCtx.shouldStop())
                    break;

                GridCacheConcurrentMap.CacheMapHolder hld = part.entriesMapIfExists(cctx.cacheId());

                if (hld == null)
                    continue;

                for (GridCacheMapEntry e : hld.map.values())
                    e.evictInternal(GridCacheVersionManager.EVICT_VER, null, false);
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override void scheduleRetry() {
            // No-op.
        }
    }
}
