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

package org.apache.ignite.internal.processors.cache.distributed.near.consistency;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cache.ReadRepairStrategy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheConsistencyViolationEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyServerNotFoundException;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_NEAR_GET_MAX_REMAPS;
import static org.apache.ignite.IgniteSystemProperties.getInteger;
import static org.apache.ignite.events.EventType.EVT_CONSISTENCY_VIOLATION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.CacheDistributedGetFutureAdapter.DFLT_MAX_REMAP_CNT;

/**
 *
 */
public abstract class GridNearReadRepairAbstractFuture extends GridFutureAdapter<Map<KeyCacheObject, EntryGetResult>> {
    /** Maximum number of attempts to remap key to the same primary node. */
    protected static final int MAX_REMAP_CNT = getInteger(IGNITE_NEAR_GET_MAX_REMAPS, DFLT_MAX_REMAP_CNT);

    /** Lsnr calls upd. */
    private static final AtomicIntegerFieldUpdater<GridNearReadRepairAbstractFuture> LSNR_CALLS_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearReadRepairAbstractFuture.class, "lsnrCalls");

    /** Remap calls upd. */
    private static final AtomicIntegerFieldUpdater<GridNearReadRepairAbstractFuture> REMAP_CALLS_UPD =
        AtomicIntegerFieldUpdater.newUpdater(GridNearReadRepairAbstractFuture.class, "remapCalls");

    /** Affinity node's get futures. */
    protected final Map<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> futs;

    /** Context. */
    protected final GridCacheContext<KeyCacheObject, EntryGetResult> ctx;

    /** Context. */
    protected final Collection<KeyCacheObject> keys;

    /** Read through flag. */
    protected final boolean readThrough;

    /** Task name. */
    protected final String taskName;

    /** Deserialize binary flag. */
    protected final boolean deserializeBinary;

    /** Recovery flag. */
    protected final boolean recovery;

    /** Expiry policy flag. */
    protected final IgniteCacheExpiryPolicy expiryPlc;

    /** Tx. */
    protected final IgniteInternalTx tx;

    /** Primaries per key. */
    protected final Map<KeyCacheObject, ClusterNode> primaries;

    /** Strategy. */
    protected final ReadRepairStrategy strategy;

    /** Remap count. */
    protected final int remapCnt;

    /** Latest mapped topology version. */
    private final AffinityTopologyVersion topVer;

    /** Listener calls. */
    private volatile int lsnrCalls;

    /** Remap calls. */
    private volatile int remapCalls;

    /**
     * Creates a new instance of GridNearReadRepairAbstractFuture.
     *
     * @param topVer Topology version.
     * @param ctx Cache context.
     * @param keys Keys.
     * @param strategy Read repair strategy.
     * @param readThrough Read-through flag.
     * @param taskName Task name.
     * @param deserializeBinary Deserialize binary flag.
     * @param recovery Partition recovery flag.
     * @param expiryPlc Expiry policy.
     * @param tx Transaction. Can be {@code null} in case of atomic cache.
     * @param remappedFut Remapped future.
     */
    protected GridNearReadRepairAbstractFuture(
        AffinityTopologyVersion topVer,
        GridCacheContext<KeyCacheObject, EntryGetResult> ctx,
        Collection<KeyCacheObject> keys,
        ReadRepairStrategy strategy,
        boolean readThrough,
        String taskName,
        boolean deserializeBinary,
        boolean recovery,
        IgniteCacheExpiryPolicy expiryPlc,
        IgniteInternalTx tx,
        GridNearReadRepairAbstractFuture remappedFut) {
        this.ctx = ctx;
        this.keys = Collections.unmodifiableCollection(keys);
        this.readThrough = readThrough;
        this.taskName = taskName;
        this.deserializeBinary = deserializeBinary;
        this.recovery = recovery;
        this.expiryPlc = expiryPlc;
        this.tx = tx;

        assert strategy != null;

        this.strategy = strategy;

        remapCnt = remappedFut != null ? remappedFut.remapCnt + 1 : 0;

        this.topVer = topVer == null ? ctx.affinity().affinityTopologyVersion() : topVer;

        Map<KeyCacheObject, ClusterNode> primaries = new HashMap<>();

        Map<ClusterNode, Collection<KeyCacheObject>> mappings = new HashMap<>();

        for (KeyCacheObject key : keys) {
            List<ClusterNode> nodes = ctx.affinity().nodesByKey(key, this.topVer);

            primaries.put(key, nodes.get(0));

            for (ClusterNode node : nodes)
                mappings.computeIfAbsent(node, k -> new HashSet<>()).add(key);
        }

        if (mappings.isEmpty())
            onDone(new ClusterTopologyServerNotFoundException("Failed to map keys for cache " +
                "(all partition nodes left the grid) [topVer=" + this.topVer + ", cache=" + ctx.name() + ']'));

        this.primaries = Collections.unmodifiableMap(primaries);

        Map<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> futs = new HashMap<>();

        for (Map.Entry<ClusterNode, Collection<KeyCacheObject>> mapping : mappings.entrySet()) {
            ClusterNode node = mapping.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut =
                new GridPartitionedGetFuture<>(
                    ctx,
                    mapping.getValue(), // Keys.
                    readThrough,
                    false, // Local get required.
                    taskName,
                    deserializeBinary,
                    recovery,
                    expiryPlc,
                    false,
                    true,
                    true,
                    tx != null ? tx.label() : null,
                    tx != null ? tx.mvccSnapshot() : null,
                    node);

            futs.put(mapping.getKey(), fut);

            fut.listen(this::onResult);
        }

        this.futs = Collections.unmodifiableMap(futs);

        IgniteInternalTx prevTx = ctx.tm().tx(tx); // Within the original tx.

        try {
            for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
                assert !fut.initialized();

                fut.init(topVer);
            }
        }
        finally {
            ctx.tm().tx(prevTx);
        }
    }

    /**
     * @param topVer Topology version.
     */
    protected void remap(AffinityTopologyVersion topVer) {
        assert !isDone();

        if (REMAP_CALLS_UPD.compareAndSet(this, 0, 1)) {
            GridNearReadRepairAbstractFuture fut = remapFuture(topVer);

            fut.listen(f -> {
                assert !isDone();

                onDone(f.result(), f.error());
            });
        }
    }

    /**
     * @param topVer Topology version.
     */
    protected abstract GridNearReadRepairAbstractFuture remapFuture(AffinityTopologyVersion topVer);

    /**
     * Collects results of each 'get' future and prepares an overall result of the operation.
     *
     * @param finished Future represents a result of GET operation.
     */
    protected final void onResult(IgniteInternalFuture<Map<KeyCacheObject, EntryGetResult>> finished) {
        if (finished.error() != null) {
            if (finished.error() instanceof ClusterTopologyServerNotFoundException)
                onDone(new UnsupportedOperationException("Operation can not be performed on unstable topology.", finished.error()));
            else
                onDone(finished.error());
        }
        else {
            if (LSNR_CALLS_UPD.incrementAndGet(this) == futs.size()) {
                assert remapCalls == 0 : remapCalls;

                assert !isDone();

                reduce();
            }
        }
    }

    /**
     * Reduces fut's results.
     */
    protected abstract void reduce();

    /**
     * Checks consistency.
     *
     * @return Regular `get` result when data is consistent.
     */
    protected final Map<KeyCacheObject, EntryGetResult> check() throws IgniteCheckedException {
        Map<KeyCacheObject, EntryGetResult> resMap = new HashMap<>(keys.size());
        Set<KeyCacheObject> inconsistentKeys = new HashSet<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : fut.keys()) {
                EntryGetResult curRes = fut.result().get(key);

                if (!resMap.containsKey(key)) {
                    resMap.put(key, curRes);

                    continue;
                }

                EntryGetResult prevRes = resMap.get(key);

                if (curRes != null) {
                    if (prevRes == null || prevRes.version().compareTo(curRes.version()) != 0)
                        inconsistentKeys.add(key);
                    else {
                        CacheObject curVal = curRes.value();
                        CacheObject prevVal = prevRes.value();

                        byte[] curBytes = curVal.valueBytes(ctx.cacheObjectContext());
                        byte[] prevBytes = prevVal.valueBytes(ctx.cacheObjectContext());

                        if (!Arrays.equals(curBytes, prevBytes))
                            inconsistentKeys.add(key);

                    }
                }
                else if (prevRes != null)
                    inconsistentKeys.add(key);
            }
        }

        if (!inconsistentKeys.isEmpty())
            throw new IgniteConsistencyViolationException(inconsistentKeys);

        return resMap;
    }

    /**
     * @param fixedEntries Fixed map.
     */
    protected final void recordConsistencyViolation(
        Collection<KeyCacheObject> inconsistentKeys,
        Map<KeyCacheObject, EntryGetResult> fixedEntries,
        ReadRepairStrategy strategy
    ) {
        GridEventStorageManager evtMgr = ctx.gridEvents();

        if (!evtMgr.isRecordable(EVT_CONSISTENCY_VIOLATION))
            return;

        Map<Object, Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo>> entries = new HashMap<>();

        for (Map.Entry<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> pair : futs.entrySet()) {
            ClusterNode node = pair.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut = pair.getValue();

            for (KeyCacheObject key : fut.keys()) {
                if (inconsistentKeys.contains(key)) {
                    Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> map =
                        entries.computeIfAbsent(
                            ctx.unwrapBinaryIfNeeded(key, !deserializeBinary, false, null), k -> new HashMap<>());

                    EntryGetResult res = fut.result().get(key);
                    CacheEntryVersion ver = res != null ? res.version() : null;

                    Object val = res != null ? ctx.unwrapBinaryIfNeeded(res.value(), !deserializeBinary, false, null) : null;

                    boolean primary = primaries.get(key).equals(fut.affNode());
                    boolean correct = fixedEntries != null &&
                        ((fixedEntries.get(key) != null && fixedEntries.get(key).equals(res)) ||
                            (fixedEntries.get(key) == null && res == null));

                    map.put(node, new EventEntryInfo(val, ver, primary, correct));
                }
            }
        }

        Map<Object, Object> fixed;

        if (fixedEntries == null)
            fixed = Collections.emptyMap();
        else {
            fixed = new HashMap<>();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : fixedEntries.entrySet()) {
                Object key = ctx.unwrapBinaryIfNeeded(entry.getKey(), !deserializeBinary, false, null);
                Object val = entry.getValue() != null ?
                    ctx.unwrapBinaryIfNeeded(entry.getValue().value(), !deserializeBinary, false, null) : null;

                fixed.put(key, val);
            }
        }

        evtMgr.record(new CacheConsistencyViolationEvent(
            ctx.name(),
            ctx.discovery().localNode(),
            "Consistency violation was " + (fixed == null ? "NOT " : "") + "fixed.",
            entries,
            fixed,
            strategy));
    }

    /**
     *
     */
    private static final class EventEntryInfo implements CacheConsistencyViolationEvent.EntryInfo {
        /** Value. */
        final Object val;

        /** Version. */
        final CacheEntryVersion ver;

        /** Located at the primary. */
        final boolean primary;

        /** Marked as correct during the fix. */
        final boolean correct;

        /**
         * @param val Value.
         * @param ver Version.
         * @param primary Primary.
         * @param correct Chosen.
         */
        public EventEntryInfo(Object val, CacheEntryVersion ver, boolean primary, boolean correct) {
            this.val = val;
            this.ver = ver;
            this.primary = primary;
            this.correct = correct;
        }

        /** {@inheritDoc} */
        @Override public Object getValue() {
            return val;
        }


        /** {@inheritDoc} */
        @Override public CacheEntryVersion getVersion() {
            return ver;
        }


        /** {@inheritDoc} */
        @Override public boolean isPrimary() {
            return primary;
        }


        /** {@inheritDoc} */
        @Override public boolean isCorrect() {
            return correct;
        }
    }
}
