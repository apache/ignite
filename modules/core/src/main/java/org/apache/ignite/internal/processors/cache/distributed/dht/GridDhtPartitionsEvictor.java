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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class that serves asynchronous partition eviction process.
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

    /** Queue contains partitions scheduled for eviction. */
    private final ConcurrentHashMap<Integer, GridDhtLocalPartition> evictionQueue = new ConcurrentHashMap<>();

    /** Flag indicates that eviction process is running at the moment, false in other case. */
    private final AtomicBoolean evictionRunning = new AtomicBoolean();

    /** Future for currently running eviction. */
    private volatile IgniteInternalFuture<Boolean> evictionFuture;

    /** Eviction progress frequency in ms. */
    private final long evictionProgressFreqMs = IgniteSystemProperties.getLong(SHOW_EVICTION_PROGRESS_FREQ,
        DEFAULT_SHOW_EVICTION_PROGRESS_FREQ_MS);

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

    private final GPC<Boolean> evictionTask = () -> {
        boolean locked = true;

        long nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;

        while (locked || !evictionQueue.isEmpty()) {
            if (!locked && !evictionRunning.compareAndSet(false, true))
                return false;

            try {
                for (GridDhtLocalPartition part : evictionQueue.values()) {
                    // Show progress of currently evicting partitions.
                    if (U.currentTimeMillis() >= nextShowProgressTime) {
                        if (log.isInfoEnabled())
                            log.info("Eviction in progress [grp=" + grp.cacheOrGroupName()
                                + ", remainingCnt=" + evictionQueue.size() + "]");

                        nextShowProgressTime = U.currentTimeMillis() + evictionProgressFreqMs;
                    }

                    try {
                        boolean success = part.tryClear();

                        if (success) {
                            evictionQueue.remove(part.id());

                            if (part.state() == GridDhtPartitionState.EVICTED && part.markForDestroy())
                                part.destroy();
                        }
                    }
                    catch (Throwable ex) {
                        if (ctx.kernalContext().isStopping()) {
                            LT.warn(log, ex, "Partition eviction failed (current node is stopping).",
                                false,
                                true);

                            evictionQueue.clear();

                            return true;
                        }
                        else
                            LT.error(log, ex, "Partition eviction failed, this can cause grid hang.");
                    }
                }
            }
            finally {
                if (!evictionQueue.isEmpty()) {
                    if (ctx.kernalContext().isStopping()) {
                        evictionQueue.clear();

                        locked = false;
                    }
                    else
                        locked = true;
                }
                else {
                    boolean res = evictionRunning.compareAndSet(true, false);

                    assert res;

                    locked = false;
                }
            }
        }

        return true;
    };

    /**
     * Adds partition to eviction queue and starts eviction process.
     *
     * @param part Partition to evict.
     */
    public void evictPartitionAsync(GridDhtLocalPartition part) {
        evictionQueue.putIfAbsent(part.id(), part);

        if (evictionRunning.compareAndSet(false, true)) {
            evictionFuture = ctx.kernalContext().closure().callLocalSafe(new GPC<Boolean>() {
                @Override public Boolean call() {

                }
            }, /*system pool*/ true);
        }
    }

    /**
     *
     */
    public void stop() {

    }


}
