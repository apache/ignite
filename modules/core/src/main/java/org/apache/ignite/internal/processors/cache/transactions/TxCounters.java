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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** Size changes for cache partitions made by transaction */
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, AtomicLong>> sizeDeltas = new ConcurrentHashMap<>();

    private final Map<Integer, Map<Integer, Collector>> collectors = new HashMap<>();

    /** Update counters for cache partitions in the end of transaction */
    private List<PartitionUpdateCountersMessage> updCntrs;

    private static final Function<Integer, Map<Integer, Collector>> NEW_MAP_FUN = k -> new HashMap<>();

    private static final BiFunction<KeyCacheObject, Boolean, Boolean> ON_DELETE_FUN = (k, v) -> v == null ? Boolean.FALSE : v ? null : v;

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

    public void onCreate(int cacheId, int part, KeyCacheObject key) {
        collector(cacheId, part).onCreate(key);
    }

    public void onUpdate(int cacheId, int part, KeyCacheObject key) {
        collector(cacheId, part).onUpdate(key);
    }

    public void onDelete(int cacheId, int part, KeyCacheObject key) {
        collector(cacheId, part).onDelete(key);
    }

    private Collector collector(int cacheId, int part) {
        Map<Integer, Collector> map = collectors.computeIfAbsent(cacheId, NEW_MAP_FUN);

        Collector res = map.get(part);

        if (res != null)
            return res;

        Collector res0 = map.putIfAbsent(part, res = new Collector(cacheId, part));

        return res0 != null ? res0 : res;
    }

    /** */
    public void updateCounters(List<PartitionUpdateCountersMessage> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /** */
    public List<PartitionUpdateCountersMessage> updateCounters() {
        return updCntrs != null ? updCntrs : Collections.emptyList();
    }

    /** */
    public Map<Integer, ? extends Map<Integer, AtomicLong>> sizeDeltas() {
        return Collections.unmodifiableMap(sizeDeltas);
    }

    /**
     * @return
     */
    public Collection<PartitionUpdateRecord> partitionUpdateRecords() {
        return F.castDown(
                    F.view(
                        F.flatCollections(
                            F.transform(collectors.values(), Map::values)), c -> c.updatesCount() > 0));
    }

    public interface PartitionUpdateRecord {
        int cacheId();
        int partition();
        long updatesCount();
    }

    /** */
    private class Collector implements PartitionUpdateRecord {
        private final int cacheId;
        private final int partition;

        /** */
        private Map<KeyCacheObject, Boolean> map = new HashMap<>();

        private Collector(int cacheId, int partition) {
            this.cacheId = cacheId;
            this.partition = partition;
        }

        public void onCreate(KeyCacheObject key) {
            map.putIfAbsent(key, Boolean.TRUE);
        }

        public void onUpdate(KeyCacheObject key) {
            map.putIfAbsent(key, Boolean.FALSE);
        }

        public void onDelete(KeyCacheObject key) {
            map.compute(key, ON_DELETE_FUN);
        }

        @Override public int cacheId() {
            return cacheId;
        }

        @Override public int partition() {
            return partition;
        }

        @Override public long updatesCount() {
            // TODO may be higher than integer
            return map.size();
        }
    }
}
