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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Values which should be tracked during transaction execution and applied on commit.
 */
public class TxCounters {
    /** */
    @GridToStringExclude
    private final GridCacheSharedContext<?, ?> cctx;
    /** Size changes for cache partitions made by transaction */
    private final ConcurrentMap<Integer, ConcurrentMap<Integer, AtomicLong>> sizeDeltas = new ConcurrentHashMap<>();
    /** Update counters for cache partitions in the end of transaction */
    private Map<Integer, Map<Integer, Long>> updCntrs;

    /**
     * @param cctx Cctx.
     */
    public TxCounters(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

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

        accDelta.addAndGet(delta);
    }

    /** */
    public void updateCounters(Map<Integer, Map<Integer, Long>> updCntrs) {
        this.updCntrs = updCntrs;
    }

    /** */
    public Map<Integer, Map<Integer, Long>> updateCounters() {
        return updCntrs;
    }

    // TODO extract from here?
    /**
     * Updates local partitions size metric.
     */
    public void updateLocalPartitionSizes() {
        if (F.isEmpty(sizeDeltas))
            return;

        for (Map.Entry<Integer, ConcurrentMap<Integer, AtomicLong>> entry : sizeDeltas.entrySet()) {
            Integer cacheId = entry.getKey();
            Map<Integer, AtomicLong> partDeltas = entry.getValue();

            assert !F.isEmpty(partDeltas);

            GridDhtPartitionTopology top = cctx.cacheContext(cacheId).topology();

            for (Map.Entry<Integer, AtomicLong> e : partDeltas.entrySet()) {
                Integer p = e.getKey();
                long delta = e.getValue().get();

                GridDhtLocalPartition dhtPart = top.localPartition(p);

                assert dhtPart != null;

                dhtPart.dataStore().updateSize(cacheId, delta);
            }
        }
    }
}
