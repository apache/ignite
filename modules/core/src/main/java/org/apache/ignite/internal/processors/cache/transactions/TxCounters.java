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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCounters;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** Size changes for cache partitions made by transaction */
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, AtomicLong>> sizeDeltas = new ConcurrentHashMap<>();
    /** Update counters for cache partitions in the end of transaction */
    private Map<Integer, PartitionUpdateCounters> updCntrs;

    /**
     * Accumulates size change for cache partition.
     *
     * @param cacheId Cache id.
     * @param part Partition id.
     * @param delta Size delta.
     */
    public void accumulateSizeDelta(int cacheId, int part, long delta) {
        ConcurrentMap<Integer, AtomicLong> partDeltas = sizeDeltas.get(cacheId);

        if (partDeltas == null) {
            ConcurrentMap<Integer, AtomicLong> partDeltas0 =
                sizeDeltas.putIfAbsent(cacheId, partDeltas = new ConcurrentHashMap<>());

            if (partDeltas0 != null)
                partDeltas = partDeltas0;
        }

        AtomicLong accDelta = partDeltas.get(part);

        if (accDelta == null) {
            AtomicLong accDelta0 = partDeltas.putIfAbsent(part, accDelta = new AtomicLong());

            if (accDelta0 != null)
                accDelta = accDelta0;
        }

        // here AtomicLong is used more as a container,
        // every instance is assumed to be accessed in thread-confined manner
        accDelta.set(accDelta.get() + delta);
    }

    /** */
    public void updateCounters(Map<Integer, PartitionUpdateCounters> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /** */
    public Map<Integer, PartitionUpdateCounters> updateCounters() {
        return updCntrs != null ? Collections.unmodifiableMap(updCntrs) : Collections.emptyMap();
    }

    /** */
    public Map<Integer, ? extends Map<Integer, AtomicLong>> sizeDeltas() {
        return Collections.unmodifiableMap(sizeDeltas);
    }
}
