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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;

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
    protected final GridCacheContext<?, ?> ctx;

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

    /** Initialized flag. */
    private volatile boolean inited;

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
        GridCacheContext<?, ?> ctx,
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
                    (GridCacheContext<KeyCacheObject, EntryGetResult>)ctx,
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
                    node);

            futs.put(mapping.getKey(), fut);

            fut.listen(this::onResult);
        }

        this.futs = Collections.unmodifiableMap(futs);
    }

    /**
     *
     */
    public GridNearReadRepairAbstractFuture init() {
        assert !inited;

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

        if (!ctx.kernalContext().cache().context().exchange().lastFinishedFuture().rebalanced())
            onDone(new IllegalStateException("Operation can not be performed on unstable topology. " +
                "Rebalance is in progress?"));

        inited = true;

        return this;
    }

    /**
     * @param topVer Topology version.
     */
    protected void remap(AffinityTopologyVersion topVer) {
        assert !isDone();

        if (REMAP_CALLS_UPD.compareAndSet(this, 0, 1)) {
            GridNearReadRepairAbstractFuture fut = remapFuture(topVer);

            fut.listen(() -> {
                assert !isDone();

                onDone(fut.result(), fut.error());
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

        assert lsnrCalls <= futs.size();
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
            throw new IgniteConsistencyCheckFailedException(inconsistentKeys);

        return resMap;
    }

    /**
     * Calculates correct values.
     *
     * @param keys Keys.
     * @return Correctet entries.
     */
    protected Map<KeyCacheObject, EntryGetResult> correct(Set<KeyCacheObject> keys) throws IgniteCheckedException {
        Map<KeyCacheObject, EntryGetResult> correctedMap;

        if (strategy == ReadRepairStrategy.LWW)
            correctedMap = correctWithLww(keys);
        else if (strategy == ReadRepairStrategy.PRIMARY)
            correctedMap = correctWithPrimary(keys);
        else if (strategy == ReadRepairStrategy.RELATIVE_MAJORITY)
            correctedMap = correctWithMajority(keys);
        else if (strategy == ReadRepairStrategy.REMOVE)
            correctedMap = correctWithRemove(keys);
        else if (strategy == ReadRepairStrategy.CHECK_ONLY)
            throw new IgniteConsistencyRepairFailedException(null, keys);
        else
            throw new UnsupportedOperationException("Unsupported strategy: " + strategy);

        return correctedMap;
    }

    /**
     *
     */
    private Map<KeyCacheObject, EntryGetResult> correctWithLww(Set<KeyCacheObject> inconsistentKeys) throws IgniteCheckedException {
        Map<KeyCacheObject, EntryGetResult> newestMap = new HashMap<>(inconsistentKeys.size()); // Newest entries (by version).
        Map<KeyCacheObject, EntryGetResult> correctedMap = new HashMap<>(inconsistentKeys.size());

        Set<KeyCacheObject> irreparableSet = new HashSet<>();

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : inconsistentKeys) {
                if (!fut.keys().contains(key))
                    continue;

                EntryGetResult candidateRes = fut.result().get(key);

                boolean hasNewest = newestMap.containsKey(key);

                if (!hasNewest) {
                    newestMap.put(key, candidateRes);

                    continue;
                }

                EntryGetResult newestRes = newestMap.get(key);

                if (candidateRes != null) {
                    if (newestRes == null) {
                        if (hasNewest) // Newest is null.
                            irreparableSet.add(key);
                        else { // Existing data wins.
                            newestMap.put(key, candidateRes);
                            correctedMap.put(key, candidateRes);
                        }
                    }
                    else {
                        int compareRes = candidateRes.version().compareTo(newestRes.version());

                        if (compareRes > 0) { // Newest data wins.
                            newestMap.put(key, candidateRes);
                            correctedMap.put(key, candidateRes);
                        }
                        else if (compareRes < 0)
                            correctedMap.put(key, newestRes);
                        else if (compareRes == 0) {
                            CacheObject candidateVal = candidateRes.value();
                            CacheObject newestVal = newestRes.value();

                            byte[] candidateBytes = candidateVal.valueBytes(ctx.cacheObjectContext());
                            byte[] newestBytes = newestVal.valueBytes(ctx.cacheObjectContext());

                            if (!Arrays.equals(candidateBytes, newestBytes))
                                irreparableSet.add(key);
                        }
                    }
                }
                else if (newestRes != null)
                    irreparableSet.add(key); // Impossible to detect latest between existing and null.
            }
        }

        assert !correctedMap.containsValue(null) : "null should never be considered as a fix";

        throwRepairFailedIfNecessary(correctedMap, irreparableSet);

        return correctedMap;
    }

    /**
     *
     */
    protected Map<KeyCacheObject, EntryGetResult> correctWithPrimary(Collection<KeyCacheObject> inconsistentKeys) {
        Map<KeyCacheObject, EntryGetResult> correctedMap = new HashMap<>(inconsistentKeys.size());

        for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
            for (KeyCacheObject key : inconsistentKeys) {
                if (fut.keys().contains(key) && primaries.get(key).equals(fut.affNode()))
                    correctedMap.put(key, fut.result().get(key));
            }
        }

        return correctedMap;
    }

    /**
     *
     */
    private Map<KeyCacheObject, EntryGetResult> correctWithRemove(Collection<KeyCacheObject> inconsistentKeys) {
        Map<KeyCacheObject, EntryGetResult> correctedMap = new HashMap<>(inconsistentKeys.size());

        for (KeyCacheObject key : inconsistentKeys)
            correctedMap.put(key, null);

        return correctedMap;
    }

    /**
     *
     */
    private Map<KeyCacheObject, EntryGetResult> correctWithMajority(Collection<KeyCacheObject> inconsistentKeys)
        throws IgniteCheckedException {
        Set<KeyCacheObject> irreparableSet = new HashSet<>(inconsistentKeys.size());
        Map<KeyCacheObject, EntryGetResult> correctedMap = new HashMap<>(inconsistentKeys.size());

        for (KeyCacheObject key : inconsistentKeys) {
            Map<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> cntMap = new HashMap<>();

            for (GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut : futs.values()) {
                if (!fut.keys().contains(key))
                    continue;

                EntryGetResult res = fut.result().get(key);

                ByteArrayWrapper wrapped;
                GridCacheVersion ver;

                if (res != null) {
                    CacheObject val = res.value();

                    wrapped = new ByteArrayWrapper(val.valueBytes(ctx.cacheObjectContext()));
                    ver = res.version();
                }
                else {
                    wrapped = new ByteArrayWrapper(null);
                    ver = null;
                }

                T2<ByteArrayWrapper, GridCacheVersion> keyVer = new T2<>(wrapped, ver);

                cntMap.putIfAbsent(keyVer, new T2<>(res, 0));

                cntMap.compute(keyVer, (kv, ri) -> new T2<>(ri.getKey(), ri.getValue() + 1));
            }

            int[] sorted = cntMap.values().stream()
                .map(IgniteBiTuple::getValue)
                .sorted(Comparator.reverseOrder())
                .mapToInt(v -> v)
                .toArray();

            int max = sorted[0];

            assert max > 0;

            if (sorted.length > 1 && sorted[1] == max) { // Majority was not found.
                irreparableSet.add(key);

                continue;
            }

            for (Map.Entry<T2<ByteArrayWrapper, GridCacheVersion>, T2<EntryGetResult, Integer>> cnt : cntMap.entrySet())
                if (cnt.getValue().getValue().equals(max)) {
                    correctedMap.put(key, cnt.getValue().getKey());

                    break;
                }
        }

        throwRepairFailedIfNecessary(correctedMap, irreparableSet);

        return correctedMap;
    }

    /**
     *
     */
    private void throwRepairFailedIfNecessary(
        Map<KeyCacheObject, EntryGetResult> correctedMap,
        Set<KeyCacheObject> irreparableSet) throws IgniteConsistencyRepairFailedException {
        if (!irreparableSet.isEmpty()) {
            // Fixing raw data, correctedMap may contain keys from irreparableSet.
            correctedMap.entrySet().removeIf(entry -> irreparableSet.contains(entry.getKey()));

            throw new IgniteConsistencyRepairFailedException(correctedMap, irreparableSet);
        }
    }

    /**
     * @param repairedEntries Repaired map.
     */
    protected final void recordConsistencyViolation(
        Collection<KeyCacheObject> inconsistentKeys,
        Map<KeyCacheObject, EntryGetResult> repairedEntries
    ) {
        GridEventStorageManager evtMgr = ctx.gridEvents();

        if (!evtMgr.isRecordable(EVT_CONSISTENCY_VIOLATION))
            return;

        boolean includeSensitive = S.includeSensitive();

        Map<KeyCacheObject, Object> sensitiveKeyMap = new HashMap<>();
        Map<ByteArrayWrapper, Object> sensitiveValMap = new HashMap<>();

        Map<Object, CacheConsistencyViolationEvent.EntriesInfo> entries = new HashMap<>();

        for (Map.Entry<ClusterNode, GridPartitionedGetFuture<KeyCacheObject, EntryGetResult>> pair : futs.entrySet()) {
            ClusterNode node = pair.getKey();

            GridPartitionedGetFuture<KeyCacheObject, EntryGetResult> fut = pair.getValue();

            for (KeyCacheObject key : fut.keys()) {
                if (inconsistentKeys.contains(key)) {
                    sensitiveKeyMap.computeIfAbsent(key, k -> includeSensitive
                        ? ctx.unwrapBinaryIfNeeded(k, true, false, null)
                        : "[HIDDEN_KEY#" + UUID.randomUUID() + "]");

                    CacheConsistencyViolationEvent.EntriesInfo entriesInfo =
                        entries.computeIfAbsent(sensitiveKeyMap.get(key), k -> new EventEntriesInfo(key.partition()));

                    EntryGetResult res = fut.result().get(key);
                    CacheEntryVersion ver = res != null ? res.version() : null;

                    Object val = sensitiveValue(includeSensitive, res, sensitiveValMap);

                    boolean primary = primaries.get(key).equals(fut.affNode());
                    boolean correct = repairedEntries != null &&
                        ((repairedEntries.get(key) != null && repairedEntries.get(key).equals(res)) ||
                            (repairedEntries.get(key) == null && res == null));

                    entriesInfo.getMapping().put(node, new EventEntryInfo(val, ver, primary, correct));
                }
            }
        }

        Map<Object, Object> repaired;

        if (repairedEntries == null)
            repaired = Collections.emptyMap();
        else {
            repaired = new HashMap<>();

            for (Map.Entry<KeyCacheObject, EntryGetResult> entry : repairedEntries.entrySet()) {
                Object key = sensitiveKeyMap.get(entry.getKey());
                Object val = sensitiveValue(includeSensitive, entry.getValue(), sensitiveValMap);

                repaired.put(key, val);
            }
        }

        evtMgr.record(new CacheConsistencyViolationEvent(
            ctx.name(),
            ctx.discovery().localNode(),
            "Consistency violation was " + (repaired == null ? "NOT " : "") + "repaired.",
            entries,
            repaired,
            strategy));
    }

    /**
     *
     */
    private Object sensitiveValue(boolean includeSensitive, EntryGetResult res,
        Map<ByteArrayWrapper, Object> sensitiveValMap) {
        if (res != null) {
            CacheObject val = res.value();

            try {
                ByteArrayWrapper wrapped = new ByteArrayWrapper(val.valueBytes(ctx.cacheObjectContext()));

                return sensitiveValMap.computeIfAbsent(wrapped, w ->
                    includeSensitive ?
                        ctx.unwrapBinaryIfNeeded(val, true, false, null) :
                        "[HIDDEN_VALUE#" + UUID.randomUUID() + "]");
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to unmarshall object.", e);
            }
        }
        else
            return null;
    }

    /**
     *
     */
    private static final class EventEntriesInfo implements CacheConsistencyViolationEvent.EntriesInfo {
        /** Mapping. */
        final Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> mapping = new HashMap<>();

        /** Partition. */
        final int partition;

        /**
         * @param partition Partition.
         */
        public EventEntriesInfo(int partition) {
            this.partition = partition;
        }

        /** {@inheritDoc} */
        @Override public Map<ClusterNode, CacheConsistencyViolationEvent.EntryInfo> getMapping() {
            return mapping;
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return partition;
        }
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

    /**
     *
     */
    protected static final class ByteArrayWrapper {
        /** Array. */
        final byte[] arr;

        /** */
        public ByteArrayWrapper(byte[] arr) {
            this.arr = arr;
        }

        /** */
        @Override public boolean equals(Object o) {
            return Arrays.equals(arr, ((ByteArrayWrapper)o).arr);
        }

        /** */
        @Override public int hashCode() {
            return Arrays.hashCode(arr);
        }
    }

    /**
     *
     */
    protected static final class IgniteConsistencyCheckFailedException extends IgniteCheckedException {
        /** */
        private static final long serialVersionUID = 0L;

        /** Inconsistent entries keys. */
        private final Set<KeyCacheObject> keys;

        /**
         * @param keys Keys.
         */
        public IgniteConsistencyCheckFailedException(Set<KeyCacheObject> keys) {
            assert keys != null && !keys.isEmpty();

            this.keys = Collections.unmodifiableSet(keys);
        }

        /**
         * Inconsistent entries keys.
         */
        public Set<KeyCacheObject> keys() {
            return keys;
        }
    }

    /**
     *
     */
    protected static final class IgniteConsistencyRepairFailedException extends IgniteCheckedException {
        /** */
        private static final long serialVersionUID = 0L;

        /** Corrected entries keys. */
        private final Map<KeyCacheObject, EntryGetResult> correctedMap;

        /** Irreparable entries keys. */
        private final Set<KeyCacheObject> irreparableKeys;

        /**
         * @param correctedMap Repairable map.
         * @param irreparableKeys Irreparable keys.
         */
        public IgniteConsistencyRepairFailedException(Map<KeyCacheObject, EntryGetResult> correctedMap,
            Set<KeyCacheObject> irreparableKeys) {
            this.correctedMap = correctedMap != null ? Collections.unmodifiableMap(correctedMap) : null;
            this.irreparableKeys = Collections.unmodifiableSet(irreparableKeys);
        }

        /**
         * Repairable keys.
         */
        public Map<KeyCacheObject, EntryGetResult> correctedMap() {
            return correctedMap;
        }

        /**
         * Irreparable keys.
         */
        public Set<KeyCacheObject> irreparableKeys() {
            return irreparableKeys;
        }
    }
}
