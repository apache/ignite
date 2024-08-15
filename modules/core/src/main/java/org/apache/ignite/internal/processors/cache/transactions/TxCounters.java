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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** Size changes for cache partitions made by transaction */
    private final Map<Integer, Map<Integer, AtomicLong>> sizeDeltas = new ConcurrentHashMap<>();

    /** Per-partition update counter accumulator. */
    private final Map<Integer, Map<Integer, AtomicLong>> updCntrsAcc = new HashMap<>();

    /** Final update counters for cache partitions in the end of transaction */
    private volatile Map<Integer, PartitionUpdateCountersMessage> updCntrs;

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
        this.updCntrs = U.newHashMap(updCntrs.size());

        for (PartitionUpdateCountersMessage cntr : updCntrs)
            this.updCntrs.put(cntr.cacheId(), cntr);
    }

    /**
     * @return Final update counters.
     */
    @Nullable public Collection<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs == null ? null : updCntrs.values();
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

    /**
     * @param cacheId Cache id.
     * @param partId Partition id.
     *
     * @return Counter or {@code null} if cache partition has not updates.
     */
    public Long generateNextCounter(int cacheId, int partId) {
        PartitionUpdateCountersMessage msg = updCntrs.get(cacheId);

        if (msg == null)
            return null;

        return msg.nextCounter(partId);
    }
}
