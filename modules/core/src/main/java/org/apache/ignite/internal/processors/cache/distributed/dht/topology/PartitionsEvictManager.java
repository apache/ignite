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
     * Adds partition to eviction queue and starts eviction process if permit available.
     *
     * @param grp Group context.
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(CacheGroupContext grp, GridDhtLocalPartition part) {
        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
            grp.groupId(), (k) -> new GroupEvictionContext(grp));

        // Check node stop.
        if (grpEvictionCtx.shouldStop())
            return;

        int bucket;

        synchronized (mux) {
            ExclusiveClearFuture exclFut = grpEvictionCtx.exclClearFut;

            if (exclFut != null && exclFut.partIds.contains(part.id())) {
                // Exclusive evict has been requested earlier.
                if (part.exclusiveClearAsync(exclFut.resFut))
                    exclFut.onInit(part);

                return;
            }

            if (!grpEvictionCtx.partIds.add(part.id()))
                return;

            bucket = evictionQueue.offer(new PartitionEvictionTask(part, grpEvictionCtx));
        }

        grpEvictionCtx.totalTasks.incrementAndGet();

        if (log.isDebugEnabled())
            log.debug("Partition has been scheduled for eviction [grp=" + grp.cacheOrGroupName()
                + ", p=" + part.id() + ", state=" + part.state() + "]");

        scheduleNextPartitionEviction(bucket);
    }

    /**
     * Gets next partition from the queue and schedules it for eviction.
     *
     * @param bucket Bucket.
     */
    private void scheduleNextPartitionEviction(int bucket) {
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
                    BucketQueueTask evictionTask = evictionQueue.poll(bucket);

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

                    GroupEvictionContext grpEvictionCtx = evictionTask.context();

                    // Check that group or node stopping.
                    if (grpEvictionCtx.shouldStop())
                        continue;

                    // Get permit for this task.
                    permits--;

                    // Register task future, may need if group or node will be stopped.
                    grpEvictionCtx.taskScheduled(evictionTask);

                    evictionTask.finishFuture().listen(f -> {
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

            if (log.isInfoEnabled())
                log.info("Eviction in progress [permits=" + permits+
                    ", threads=" + threads +
                    ", groups=" + evictionGroupsMap.keySet().size() +
                    ", remainingPartsToEvict=" + size + "]");

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
    private class GroupEvictionContext implements EvictionContext {
        /** */
        private final CacheGroupContext grp;

        /** Deduplicate set partition ids. */
        private final Set<Integer> partIds = new HashSet<>();

        /** Future for exclusive clear of a set of partitions. */
        private ExclusiveClearFuture exclClearFut;

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
            return stop || sharedEvictionCtx.shouldStop();
        }

        /**
         *
         * @param task Partition eviction task.
         */
        private synchronized void taskScheduled(BucketQueueTask task) {
            if (shouldStop())
                return;

            taskInProgress++;

            IgniteInternalFuture<?> fut = task.finishFuture();

            int partId = (task instanceof PartitionEvictionTask) ? ((PartitionEvictionTask)task).part.id() : -1;

            if (partId >= 0) {
                partIds.remove(partId);

                partsEvictFutures.put(partId, fut);
            }

            fut.listen(f -> {
                synchronized (this) {
                    taskInProgress--;

                    if (partId >= 0)
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
            ExclusiveClearFuture exclFut;

            synchronized (mux) {
                exclFut = exclClearFut;

                exclClearFut = null;
            }

            if (exclFut != null) {
                exclFut.stop();

                awaitFinish(null, exclFut);
            }

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
                    ", totalParts=" + grp.topology().localPartitions().size() + "]");
        }
    }

    /**
     * Base class for all tasks to queue.
     */
    private abstract static class BucketQueueTask extends GridFutureAdapter<Void> implements Runnable {
        /** Eviction context. */
        protected final GroupEvictionContext grpEvictionCtx;

        /**
         * @param grpEvictionCtx Group eviction context.
         */
        private BucketQueueTask(GroupEvictionContext grpEvictionCtx) {
            this.grpEvictionCtx = grpEvictionCtx;
        }

        /**
         * @return Group eviction context.
         */
        GroupEvictionContext context() {
            return grpEvictionCtx;
        }

        /**
         * @return Task size.
         */
        abstract long size();

        /**
         * @return Finish future.
         */
        IgniteInternalFuture<?> finishFuture() {
            return this;
        }
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    class PartitionEvictionTask extends BucketQueueTask {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        /** */
        private final long size;

        /**
         * @param part Partition.
         * @param grpEvictionCtx Eviction context.
         */
        private PartitionEvictionTask(
            GridDhtLocalPartition part,
            GroupEvictionContext grpEvictionCtx
        ) {
            super(grpEvictionCtx);

            this.part = part;

            size = part.fullSize();
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (grpEvictionCtx.shouldStop()) {
                onDone();

                return;
            }

            try {
                boolean success = part.tryClear(grpEvictionCtx);

                if (success) {
                    if (part.state() == GridDhtPartitionState.EVICTED && part.markForDestroy())
                        part.destroy();
                }

                // Complete eviction future before schedule new to prevent deadlock with
                // simultaneous eviction stopping and scheduling new eviction.
                onDone();

                // Re-offer partition if clear was unsuccessful due to partition reservation.
                if (!success)
                    evictPartitionAsync(grpEvictionCtx.grp, part);
            }
            catch (Throwable ex) {
                onDone(ex);

                if (cctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction failed (current node is stopping).",
                        false,
                        true);
                }
                else {
                    LT.error(log, ex, "Partition eviction failed, this can cause grid hang.");

                    cctx.kernalContext().failure().process(new FailureContext(SYSTEM_WORKER_TERMINATION, ex));
                }
            }
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return size;
        }
    }

    /**
     *
     */
    class BucketQueue {
        /** */
        private final long[] bucketSizes;

        /** Queues contains partitions scheduled for eviction. */
        final Queue<BucketQueueTask>[] buckets;

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
        BucketQueueTask poll(int bucket) {
            BucketQueueTask task = buckets[bucket].poll();

            if (task != null)
                bucketSizes[bucket] -= task.size();

            return task;
        }

        /**
         * Poll eviction task from queue (bucket is not specific).
         *
         * @return Partition evict task.
         */
        BucketQueueTask pollAny() {
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
        int offer(BucketQueueTask task) {
            int bucket = calculateBucket();

            buckets[bucket].offer(task);

            bucketSizes[bucket] += task.size();

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

            for (Queue<BucketQueueTask> queue : buckets)
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
        private Queue<BucketQueueTask> createEvictPartitionQueue() {
            switch (QUEUE_TYPE) {
                case 1:
                    return new PriorityBlockingQueue<>(
                        1000, new Comparator<BucketQueueTask>() {
                            /** {@inheritDoc} */
                            @Override public int compare(BucketQueueTask a, BucketQueueTask b) {
                                int a0 = (a instanceof RowCacheCleanerTask) ? 1 : 0;
                                int b0 = (b instanceof RowCacheCleanerTask) ? 1 : 0;

                                if (a0 == b0)
                                    return Long.compare(a.size(), b.size());
                                else
                                    return Long.compare(a0, b0);
                            }
                    });
                default:
                    return new LinkedBlockingQueue<>();
            }
        }
    }

    /**
     *
     * @param grp Cache group context.
     * @param parts Partitions.
     * @throws IgniteCheckedException If failed.
     * @return Future.
     */
    public IgniteInternalFuture<Void> evictPartitionsExclusively(CacheGroupContext grp,
        List<GridDhtLocalPartition> parts) throws IgniteCheckedException {
        validateCacheGroupForExclusiveEvict(grp);

        if (F.isEmpty(parts))
            return new GridFinishedFuture<>();

        GroupEvictionContext grpEvictionCtx = evictionGroupsMap.computeIfAbsent(
            grp.groupId(), (k) -> new GroupEvictionContext(grp));

        ExclusiveClearFuture fut = new ExclusiveClearFuture(grpEvictionCtx, parts);

        synchronized (mux) {
            if (grpEvictionCtx.exclClearFut != null)
                throw new IgniteCheckedException("Only one exclusive evict can be scheduled for the same cache group." +
                    " [grpId=" + grp.groupId() + "]");

            Set<Integer> res = new HashSet<>(grpEvictionCtx.partIds);

            res.retainAll(fut.partIds);

            if (!res.isEmpty())
                throw new IgniteCheckedException("Can't schedule exclusive evict for a partition " +
                    "due to scheduled regular evict for the same partition. [grpId=" + grp.groupId() +
                    ", partIds=" + Arrays.toString(U.toIntArray(res)) + "]");

            grpEvictionCtx.exclClearFut = fut;

            fut.listen(f -> {
                synchronized (mux) {
                    grpEvictionCtx.exclClearFut = null;
                }
            });
        }

        for (GridDhtLocalPartition part : parts) {
            if (part.exclusiveClearAsync(fut.resFut))
                fut.onInit(part);
        }

        return fut;
    }

    /**
     * Context of exclusive partition evict.
     */
    private class ExclusiveClearFuture extends GridFutureAdapter<Void> {
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

        /** Tasks future */
        GridCompoundFuture<Void, Void> resFut = new GridCompoundFuture<>();

        /** Allows to cancel index tasks. */
        GridCompoundFuture<Void, Void> cancelFut = new GridCompoundFuture<>();

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param parts Partitions.
         */
        public ExclusiveClearFuture(GroupEvictionContext grpEvictionCtx, List<GridDhtLocalPartition> parts) {
            this.grpEvictionCtx = grpEvictionCtx;
            this.parts = parts;

            partIds = Collections.unmodifiableSet(
                parts.stream().mapToInt(GridDhtLocalPartition::id).boxed().collect(Collectors.toSet()));

            initCounter = new AtomicInteger(parts.size());

            resultCounter = new AtomicInteger(parts.size());
        }

        /**
         * Initializes partition for exclusive evict.
         *
         * @param part Partition.
         */
        public void onInit(GridDhtLocalPartition part) {
            if (log.isInfoEnabled())
                log.info("Partition confirmed exclusive clear. [partId=" + part.id() + "]");

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
            List<BucketQueueTask> tasks = new ArrayList<>();
            List<BucketQueueTask> idxTasks = new ArrayList<>();

            for (GridDhtLocalPartition p : parts) {
                ExclusiveEvictPerPartitionTask task = new ExclusiveEvictPerPartitionTask(grpEvictionCtx, p);

                tasks.add(task);

                resFut.add(task);
            }

            CacheGroupContext grp = grpEvictionCtx.grp;

            GridCompoundFuture<Void, Void> idxFut = null;

            if (grp.shared().kernalContext().query().moduleEnabled()) {
                GridQueryIndexing idx = grp.shared().kernalContext().query().getIndexing();

                GridQueryRowCacheCleaner rowCacheCleaner = idx.rowCacheCleaner(grp.groupId());

                if (rowCacheCleaner != null) {
                    RowCacheCleanerTask task = new RowCacheCleanerTask(grpEvictionCtx, rowCacheCleaner, partIds);

                    tasks.add(task);

                    resFut.add(task);
                }

                List<IgniteBiTuple<Runnable, IgniteInternalFuture<Void>>> idxPurgeList =
                    idx.purgeIndexPartitions(grp, partIds);

                assert idxPurgeList != null;

                if (!idxPurgeList.isEmpty()) {
                    idxFut = new GridCompoundFuture<>();

                    for (IgniteBiTuple<Runnable, IgniteInternalFuture<Void>> r : idxPurgeList) {
                        BucketQueueTask idxTask = new IndexEvictTask(grpEvictionCtx, r.get1(), r.get2());

                        cancelFut.add(r.get2());

                        idxFut.add(idxTask);

                        idxTasks.add(idxTask);
                    }

                    cancelFut.markInitialized();

                    idxFut.markInitialized();

                    resFut.add(idxFut);
                }
            }

            resFut.markInitialized();

            if (log.isInfoEnabled())
                log.info("Exclusive partition evict: scheduling tasks." +
                    " [total=" + (tasks.size() + idxTasks.size()) + ", idx=" + idxTasks.size() + "]");

            if (idxFut == null) {
                for (BucketQueueTask r0 : tasks)
                    evictionQueue.offer(r0);

                grpEvictionCtx.totalTasks.addAndGet(tasks.size());
            }
            else {
                idxFut.listen(f -> {
                    if (f.error() == null && !grpEvictionCtx.shouldStop()) {
                        for (BucketQueueTask r0 : tasks)
                            evictionQueue.offer(r0);

                        grpEvictionCtx.totalTasks.addAndGet(tasks.size());

                        scheduleNextPartitionEviction(0);
                    }
                });

                for (BucketQueueTask r0 : idxTasks)
                    evictionQueue.offer(r0);

                grpEvictionCtx.totalTasks.addAndGet(idxTasks.size());
            }

            scheduleNextPartitionEviction(0);
        }

        /**
         * Cancels index clearing tasks.
         */
        public void stop() {
            if (log.isInfoEnabled())
                log.info("Exclusive evict : stop");

            resFut.onDone(new IgniteCheckedException("Cache is stopping."));

            try {
                cancelFut.cancel();
            }
            catch (IgniteCheckedException e) {
                log.error("Error cancelling index clearing tasks", e);
            }
        }
    }

    /**
     *
     * @param grp Cache group context.
     * @throws IgniteCheckedException If failed.
     */
    private void validateCacheGroupForExclusiveEvict(CacheGroupContext grp) throws IgniteCheckedException {
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
     * Evicts partitions from single index.
     */
    private static class IndexEvictTask extends BucketQueueTask {
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
        IndexEvictTask(GroupEvictionContext grpEvictionCtx, Runnable runnable, IgniteInternalFuture<?> fut) {
            super(grpEvictionCtx);

            this.runnable = runnable;

            this.fut = fut;

            fut.listen(f -> {
                if (f.error() == null)
                    onDone();
                else
                    onDone(f.error());
            });
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (grpEvictionCtx.shouldStop()) {
                onDone();

                return;
            }

            runnable.run();
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return 0;
        }
    }

    /**
     * Removes links belonging to certain partitions from H2RowCache.
     */
    private static class RowCacheCleanerTask extends BucketQueueTask {
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
        RowCacheCleanerTask(GroupEvictionContext grpEvictionCtx, GridQueryRowCacheCleaner cleaner, Set<Integer> parts) {
            super(grpEvictionCtx);

            this.cleaner = cleaner;
            this.parts = parts;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (grpEvictionCtx.shouldStop()) {
                onDone();

                return;
            }

            try {
                cleaner.removeByPartition(parts);

                onDone();
            }
            catch (Throwable e) {
                onDone(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return 0;
        }
    }

    /**
     * Performs per-partition actions in scope of exclusive partition set evict.
     */
    private static class ExclusiveEvictPerPartitionTask extends BucketQueueTask {
        /** Partition. */
        private final GridDhtLocalPartition part;

        /**
         *
         * @param grpEvictionCtx Group eviction context.
         * @param part Partition.
         */
        ExclusiveEvictPerPartitionTask(GroupEvictionContext grpEvictionCtx, GridDhtLocalPartition part) {
            super(grpEvictionCtx);

            this.part = part;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            if (grpEvictionCtx.shouldStop()) {
                onDone();

                return;
            }

            try {
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

                onDone();
            }
            catch (Throwable e) {
                onDone(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long size() {
            return part.fullSize();
        }
    }
}
