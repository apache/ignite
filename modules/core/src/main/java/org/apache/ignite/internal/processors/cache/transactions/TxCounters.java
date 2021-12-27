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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** */
    public static final long UNKNOWN_VALUE = -1;

    /** Size changes for cache partitions made by transaction */
    private volatile Map<Long, AtomicLong> sizeDeltas;

    /** Per-partition update counter accumulator. */
    private volatile Map<Long, AtomicLong> updCntrsAcc;

    /** Final update counters for cache partitions in the end of transaction */
    private volatile Collection<PartitionUpdateCountersMessage> updCntrs;

    /** Counter tracking number of entries locked by tx. */
    private final AtomicInteger lockCntr = new AtomicInteger();

    /**
     * Accumulates size change for cache partition.
     *
     * @param cacheId Cache id.
     * @param part Partition id.
     * @param delta Size delta.
     */
    public void accumulateSizeDelta(int cacheId, int part, long delta) {
        if (sizeDeltas == null)
            sizeDeltas = new ConcurrentHashMap<>();

        AtomicLong accDelta = accumulator(sizeDeltas, cacheId, part);

        // here AtomicLong is used more as a container,
        // every instance is assumed to be accessed in thread-confined manner
        accDelta.set(accDelta.get() + delta);
    }

    /**
     * @return Map of size changes for cache partitions made by transaction.
     */
    public Map<Long, AtomicLong> sizeDeltas() {
        return sizeDeltas == null ? Collections.emptyMap() : sizeDeltas;
    }

    /**
     * @param updCntrs Final update counters.
     */
    public void updateCounters(Collection<PartitionUpdateCountersMessage> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /**
     * @return Final update counters.
     */
    @Nullable public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
    }

    /**
     * @return Accumulated update counters.
     */
    public Map<Long, AtomicLong> accumulatedUpdateCounters() {
        return updCntrsAcc == null ? Collections.emptyMap() : updCntrsAcc;
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void incrementUpdateCounter(int cacheId, int part) {
        if (updCntrsAcc == null)
            updCntrsAcc = new ConcurrentHashMap<>();

        accumulator(updCntrsAcc, cacheId, part).incrementAndGet();
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void decrementUpdateCounter(int cacheId, int part) {
        if (updCntrsAcc == null)
            updCntrsAcc = new ConcurrentHashMap<>();

        long acc = accumulator(updCntrsAcc, cacheId, part).decrementAndGet();

        assert acc >= 0;
    }

    /**
     * @param accMap Map to obtain accumulator from.
     * @param cacheId Cache id.
     * @param part Partition number.
     * @return Accumulator.
     */
    private AtomicLong accumulator(Map<Long, AtomicLong> accMap, int cacheId, int part) {
        return accMap.computeIfAbsent(U.toLong(cacheId, part), key -> new AtomicLong());
    }

    /**
     * Increments lock counter.
     */
    public void incrementLockCounter() {
        lockCntr.incrementAndGet();
    }

    /**
     * @return Current value of lock counter.
     */
    public int lockCounter() {
        return lockCntr.get();
    }

    /**
     * @param cacheId Cache id.
     * @param partId Partition id.
     *
     * @return Counter or {@code null} if cache partition has not updates.
     */
    public long generateNextCounter(int cacheId, int partId) {
        for (PartitionUpdateCountersMessage msg : updCntrs) {
            if (msg.cacheId() == cacheId)
                return msg.nextCounter(partId);
        }

        return UNKNOWN_VALUE;
    }
}
