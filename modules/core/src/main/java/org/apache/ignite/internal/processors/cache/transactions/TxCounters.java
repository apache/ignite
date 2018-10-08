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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** Size changes for cache partitions made by transaction */
    private final Map<Integer, Map<Integer, AtomicLong>> sizeDeltas = new ConcurrentHashMap<>();

    /** Per-partition update counter accumulator. */
    private final Map<Integer, Map<Integer, AtomicLong>> updCntrsAcc = new HashMap<>();

    /** Final update counters for cache partitions in the end of transaction */
    private Collection<PartitionUpdateCountersMessage> updCntrs;

    /**
     * Accumulates size change for cache partition.
     *
     * @param cacheId Cache id.
     * @param part Partition id.
     * @param delta Size delta.
     */
    public void accumulateSizeDelta(int cacheId, int part, long delta) {
        AtomicLong accDelta = accumulator(sizeDeltas, cacheId, part);

        // here AtomicLong is used more as a container,
        // every instance is assumed to be accessed in thread-confined manner
        accDelta.set(accDelta.get() + delta);
    }

    /**
     * @return Map of size changes for cache partitions made by transaction.
     */
    public Map<Integer, Map<Integer, AtomicLong>> sizeDeltas() {
        return sizeDeltas;
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
    public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs;
    }

    /**
     * @return Accumulated update counters.
     */
    public Map<Integer, Map<Integer, AtomicLong>> accumulatedUpdateCounters() {
        return updCntrsAcc;
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void incrementUpdateCounter(int cacheId, int part) {
        accumulator(updCntrsAcc, cacheId, part).incrementAndGet();
    }

    /**
     * @param cacheId Cache id.
     * @param part Partition number.
     */
    public void decrementUpdateCounter(int cacheId, int part) {
        long acc = accumulator(updCntrsAcc, cacheId, part).decrementAndGet();

        assert acc >= 0;
    }

    /**
     * @param accMap Map to obtain accumulator from.
     * @param cacheId Cache id.
     * @param part Partition number.
     * @return Accumulator.
     */
    private AtomicLong accumulator(Map<Integer, Map<Integer, AtomicLong>> accMap, int cacheId, int part) {
        Map<Integer, AtomicLong> cacheAccs = accMap.get(cacheId);

        if (cacheAccs == null) {
            Map<Integer, AtomicLong> cacheAccs0 =
                accMap.putIfAbsent(cacheId, cacheAccs = new ConcurrentHashMap<>());

            if (cacheAccs0 != null)
                cacheAccs = cacheAccs0;
        }

        AtomicLong acc = cacheAccs.get(part);

        if (acc == null) {
            AtomicLong accDelta0 = cacheAccs.putIfAbsent(part, acc = new AtomicLong());

            if (accDelta0 != null)
                acc = accDelta0;
        }

        return acc;
    }
}
