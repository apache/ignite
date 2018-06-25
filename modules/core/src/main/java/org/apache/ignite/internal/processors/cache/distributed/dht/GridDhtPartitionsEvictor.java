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

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class that serves asynchronous part eviction process.
 * Only one partition from group can be evicted at the moment.
 */
public class GridDhtPartitionsEvictor {
    /** Default eviction progress show frequency. */
    private static final int DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS = 2 * 60 * 1000; // 2 Minutes.

    /** Eviction progress frequency property name. */
    private static final String SHOW_EVICTION_PROGRESS_FREQ = "SHOW_EVICTION_PROGRESS_FREQ";

    /** */
    private final GridCacheSharedContext<?, ?> ctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Lock object. */
    private final Object mux = new Object();

    /** Queue contains partitions scheduled for eviction. */
    private final DeduplicationQueue<Integer, GridDhtLocalPartition> evictionQueue = new DeduplicationQueue<>(GridDhtLocalPartition::id);

    /**
     * Flag indicates that eviction process is running at the moment.
     * This is needed to schedule partition eviction if there are no currently running self-scheduling eviction tasks.
     * Guarded by {@link #mux}.
     */
    private boolean evictionRunning;

    /** Flag indicates that eviction process has stopped. */
    private volatile boolean stop;

    /** Future for currently running partition eviction task. */
    private volatile GridFutureAdapter<Boolean> evictionFut;

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs = IgniteSystemProperties.getLong(SHOW_EVICTION_PROGRESS_FREQ,
        DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

    /** Next time of show eviction progress. */
    private long nextShowProgressTime;

    /**
     * Constructor.
     *
     * @param grp Cache group context.
     */
    public GridDhtPartitionsEvictor(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;
        this.ctx = grp.shared();

        this.log = ctx.logger(getClass());
    }

    /**
     * Adds partition to eviction queue and starts eviction process.
     *
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(GridDhtLocalPartition part) {
        if (stop)
            return;

        boolean added = evictionQueue.offer(part);

        if (!added)
            return;

        synchronized (mux) {
            if (!evictionRunning) {
                nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;

                scheduleNextPartitionEviction();
            }
        }
    }

    /**
     * Stops eviction process.
     * Method awaits last offered partition eviction.
     */
    public void stop() {
        stop = true;

        synchronized (mux) {
            // Wait for last offered partition eviction completion.
            IgniteInternalFuture<Boolean> evictionFut0 = evictionFut;

            if (evictionFut0 != null) {
                try {
                    evictionFut0.get();
                }
                catch (IgniteCheckedException e) {
                    if (log.isDebugEnabled())
                        log.warning("Failed to await partition eviction during stopping", e);
                }
            }
        }
    }

    /**
     * Gets next partition from the queue and schedules it for eviction.
     */
    private void scheduleNextPartitionEviction() {
        if (stop)
            return;

        synchronized (mux) {
            GridDhtLocalPartition next = evictionQueue.poll();

            if (next != null) {
                showProgress();

                evictionFut = new GridFutureAdapter<>();

                ctx.kernalContext().closure().callLocalSafe(new PartitionEvictionTask(next, () -> stop), true);
            }
            else
                evictionRunning = false;
        }
    }

    /**
     * Shows progress of eviction.
     */
    private void showProgress() {
        if (U.currentTimeMillis() >= nextShowProgressTime) {
            int size = evictionQueue.size() + 1; // Queue size plus current partition.

            if (log.isInfoEnabled())
                log.info("Eviction in progress [grp=" + grp.cacheOrGroupName()
                    + ", remainingPartsCnt=" + size + "]");

            nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;
        }
    }

    /**
     * Task for self-scheduled partition eviction / clearing.
     */
    private class PartitionEvictionTask implements Callable<Boolean> {
        /** Partition to evict. */
        private final GridDhtLocalPartition part;

        /** Eviction context. */
        private final EvictionContext evictionCtx;

        /**
         * @param part Partition.
         * @param evictionCtx Eviction context.
         */
        public PartitionEvictionTask(GridDhtLocalPartition part, EvictionContext evictionCtx) {
            this.part = part;
            this.evictionCtx = evictionCtx;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws Exception {
            if (stop) {
                evictionFut.onDone();

                return false;
            }

            try {
                boolean success = part.tryClear(evictionCtx);

                if (success) {
                    if (part.state() == GridDhtPartitionState.EVICTED && part.markForDestroy())
                        part.destroy();
                }
                else // Re-offer partition if clear was unsuccessful due to partition reservation.
                    evictionQueue.offer(part);

                // Complete eviction future before schedule new to prevent deadlock with
                // simultaneous eviction stopping and scheduling new eviction.
                evictionFut.onDone();

                scheduleNextPartitionEviction();

                return true;
            }
            catch (Throwable ex) {
                evictionFut.onDone(ex);

                if (ctx.kernalContext().isStopping()) {
                    LT.warn(log, ex, "Partition eviction failed (current node is stopping).",
                        false,
                        true);
                }
                else
                    LT.error(log, ex, "Partition eviction failed, this can cause grid hang.");
            }

            return false;
        }
    }

    /**
     * Thread-safe blocking queue with items deduplication.
     *
     * @param <K> Key type of item used for deduplication.
     * @param <V> Queue item type.
     */
    private static class DeduplicationQueue<K, V> {
        /** Queue. */
        private final Queue<V> queue;

        /** Unique items set. */
        private final Set<K> uniqueItems;

        /** Key mapping function. */
        private final Function<V, K> keyMappingFunction;

        /**
         * Constructor.
         *
         * @param keyExtractor Function to extract a key from a queue item.
         *                     This key is used for deduplication if some item has offered twice.
         */
        public DeduplicationQueue(Function<V, K> keyExtractor) {
            keyMappingFunction = keyExtractor;
            queue = new LinkedBlockingQueue<>();
            uniqueItems = new GridConcurrentHashSet<>();
        }

        /**
         * Offers item to the queue.
         *
         * @param item Item.
         * @return {@code true} if item has been successfully offered to the queue,
         *         {@code false} if item was rejected because already exists in the queue.
         */
        public boolean offer(V item) {
            K key = keyMappingFunction.apply(item);

            if (uniqueItems.add(key)) {
                queue.offer(item);

                return true;
            }

            return false;
        }

        /**
         * Polls next item from queue.
         *
         * @return Next item or {@code null} if queue is empty.
         */
        public V poll() {
            V item = queue.poll();

            if (item != null) {
                K key = keyMappingFunction.apply(item);

                uniqueItems.remove(key);
            }

            return item;
        }

        /**
         * @return Size of queue.
         */
        public int size() {
            return queue.size();
        }
    }
}
