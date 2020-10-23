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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.metric.sources.CacheMetricSource;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for cache metrics.
 */
public class CacheMetricsImpl implements CacheMetrics {
    /** */
    private static final long NANOS_IN_MICROSECOND = 1000L;

    /** Cache metric source. */
    private final CacheMetricSource metricSrc;

    //TODO: Move to metric source
    /** Number of currently evicting non-affinity partitions. Not available in the old metrics framework. */
    private AtomicLongMetric evictingPartitions;

    //TODO: Move to metric source
    /** Commit time. */
    private HistogramMetricImpl commitTime;

    //TODO: Move to metric source
    /** Rollback time. */
    private HistogramMetricImpl rollbackTime;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /** DHT context. */
    private GridCacheContext<?, ?> dhtCtx;

    /** Write-behind store, if configured. */
    private GridCacheWriteBehindStore<?, ?> store;

    //TODO: BEGIN: Support all below
    /** Tx collisions info. */
    private volatile Supplier<List<Map.Entry</* Colliding keys. */ GridCacheMapEntry, /* Collisions queue size. */ Integer>>> txKeyCollisionInfo;

    /** Offheap entries count. */
    private LongGauge offHeapEntriesCnt;

    /** Offheap primary entries count. */
    private LongGauge offHeapPrimaryEntriesCnt;

    /** Offheap backup entries count. */
    private LongGauge offHeapBackupEntriesCnt;

    /** Onheap entries count. */
    private LongGauge heapEntriesCnt;

    /** Cache size. */
    private LongGauge cacheSize;

    /** Number of keys processed during index rebuilding. */
    private LongAdderMetric idxRebuildKeyProcessed;
    //TODO: END: Support all below

    /**
     * Creates cache metrics.
     *
     * @param cctx Cache context.
     */
    public CacheMetricsImpl(CacheMetricSource metricSrc, GridCacheContext<?, ?> cctx) {
        this(metricSrc, cctx, false);
    }

    /**
     * Creates cache metrics.
     *
     * @param cctx Cache context.
     * @param isNear Is near flag.
     */
    public CacheMetricsImpl(CacheMetricSource metricSrc, GridCacheContext<?, ?> cctx, boolean isNear) {
        assert cctx != null;

        this.metricSrc = metricSrc;

        this.cctx = cctx;

        if (cctx.isNear())
            dhtCtx = cctx.near().dht().context();

        if (cctx.store().store() instanceof GridCacheWriteBehindStore)
            store = (GridCacheWriteBehindStore<?, ?>)cctx.store().store();

        //TODO: Support new metrics
/*
        commitTime = mreg.histogram("CommitTime", HISTOGRAM_BUCKETS, "Commit time in nanoseconds.");

        rollbackTime = mreg.histogram("RollbackTime", HISTOGRAM_BUCKETS, "Rollback time in nanoseconds.");

        mreg.register("TxKeyCollisions", this::getTxKeyCollisions, String.class, "Tx key collisions. " +
                "Show keys and collisions queue size. Due transactional payload some keys become hot. Metric shows " +
                "corresponding keys.");

        offHeapEntriesCnt = mreg.register("OffHeapEntriesCount",
                () -> getEntriesStat().offHeapEntriesCount(), "Offheap entries count.");

        offHeapPrimaryEntriesCnt = mreg.register("OffHeapPrimaryEntriesCount",
                () -> getEntriesStat().offHeapPrimaryEntriesCount(), "Offheap primary entries count.");

        offHeapBackupEntriesCnt = mreg.register("OffHeapBackupEntriesCount",
                () -> getEntriesStat().offHeapBackupEntriesCount(), "Offheap backup entries count.");

        heapEntriesCnt = mreg.register("HeapEntriesCount",
                () -> getEntriesStat().heapEntriesCount(), "Onheap entries count.");

        cacheSize = mreg.register("CacheSize",
                () -> getEntriesStat().cacheSize(), "Local cache size.");

        idxRebuildKeyProcessed = mreg.longAdderMetric("IndexRebuildKeyProcessed",
                "Number of keys processed during index rebuilding.");

*/
    }

    /**
     * @return Cache metric source.
     */
    public CacheMetricSource metricSource() {
        return metricSrc;
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(CacheMetricsImpl delegate) {
        if (delegate != null)
            metricSrc.delegate(delegate.metricSrc);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cctx.name();
    }

    /**
     * @deprecated Invalid metric since Apache Ignite 2.0
     */
    @Deprecated
    @Override public long getOffHeapGets() {
        return -1;
    }

    /**
     * @deprecated Invalid metric since Apache Ignite 2.0
     */
    @Deprecated
    @Override public long getOffHeapPuts() {
        return -1;
    }

    /**
     * @deprecated Invalid metric since Apache Ignite 2.0
     */
    @Deprecated
    @Override public long getOffHeapRemovals() {
        return -1;
    }

    /**
     * @deprecated Invalid metric since Apache Ignite 2.0
     */
    @Deprecated
    @Override public long getOffHeapEvictions() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return metricSrc.offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return metricSrc.heapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return metricSrc.offHeapPrimaryEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return metricSrc.offHeapBackupEntriesCount();
    }

    /**
     * @deprecated Invalid metric since Apache Ignite 2.0
     */
    @Deprecated
    @Override public long getOffHeapAllocatedSize() {
        return -1;
    }

    /**
     * @deprecated Use {{@link #getCacheSize()}} instead.
     */
    @Deprecated
    @Override public int getSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return metricSrc.localSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    @Override public int getKeySize() {
        return -1;
    }

    /**
     * @deprecated It equals to {@link #getOffHeapEntriesCount()} equals 0.
     */
    @Deprecated
    @Override public boolean isEmpty() {
        return getOffHeapEntriesCount() == 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getDhtEvictQueueCurrentSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxCommitQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxThreadMapSize() {
        return cctx.tm().threadMapSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxXidMapSize() {
        return cctx.tm().idMapSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxPrepareQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxStartVersionCountsSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxCommittedVersionsSize() {
        return cctx.tm().completedVersionsSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxRolledbackVersionsSize() {
        return cctx.tm().completedVersionsSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtThreadMapSize() {
        return cctx.tm().threadMapSize();
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtXidMapSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().idMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtCommitQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtPrepareQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtStartVersionCountsSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtCommittedVersionsSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().completedVersionsSize() : -1;
    }

    /** {@inheritDoc} */
    @Deprecated
    //TODO: Must be moved to transaction metrics.
    @Override public int getTxDhtRolledbackVersionsSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().completedVersionsSize() : -1;
    }

    /**
     * @deprecated Doesn't make sense. All related metrics will have {@code -1} value if store isn't configured.
     * E.g {@link #getWriteBehindFlushSize}.
     */
    @Deprecated
    @Override public boolean isWriteBehindEnabled() {
        return store != null;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushSize() {
        return store != null ? store.getWriteBehindFlushSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindFlushThreadCount() {
        return store != null ? store.getWriteBehindFlushThreadCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public long getWriteBehindFlushFrequency() {
        return store != null ? store.getWriteBehindFlushFrequency() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindStoreBatchSize() {
        return store != null ? store.getWriteBehindStoreBatchSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindTotalCriticalOverflowCount() {
        return store != null ? store.getWriteBehindTotalCriticalOverflowCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindCriticalOverflowCount() {
        return store != null ? store.getWriteBehindCriticalOverflowCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindErrorRetryCount() {
        return store != null ? store.getWriteBehindErrorRetryCount() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getWriteBehindBufferSize() {
        return metricSrc.writeBehindFlushSize();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getAverageTxCommitTime() {
        long timeNanos = metricSrc.commitTimeNanos();

        long commitsCnt = metricSrc.txCommits();

        if (timeNanos == 0 || commitsCnt == 0)
            return 0;

        return ((1f * timeNanos) / commitsCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getAverageTxRollbackTime() {
        if (!metricSrc.enabled())
            return 0;

        long timeNanos = metricSrc.rollbackTimeNanos();

        long rollbacksCnt = metricSrc.txRollbacks();

        if (timeNanos == 0 || rollbacksCnt == 0)
            return 0;

        return ((1f * timeNanos) / rollbacksCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return metricSrc.txCommits();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return metricSrc.txRollbacks();
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        metricSrc.reset();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return metricSrc.cacheHits();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getCacheHitPercentage() {
        if (!metricSrc.enabled())
            return 0;

        long hits0 = metricSrc.cacheHits();

        long gets0 = metricSrc.reads();

        if (hits0 == 0)
            return 0;

        return (float)hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return metricSrc.misses();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getCacheMissPercentage() {
        if (!metricSrc.enabled())
            return 0;

        long misses0 = metricSrc.misses();

        long reads0 = metricSrc.reads();

        if (misses0 == 0)
            return 0;

        return (float)misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return metricSrc.reads();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return metricSrc.writes();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorPuts() {
        return metricSrc.entryProcessorPuts();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorRemovals() {
        return metricSrc.entryProcessorRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorReadOnlyInvocations() {
        return metricSrc.entryProcessorReadOnlyInvocations();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public long getEntryProcessorInvocations() {
        if (!metricSrc.enabled())
            return 0;

        return metricSrc.entryProcessorReadOnlyInvocations() +
                metricSrc.entryProcessorPuts() +
                metricSrc.entryProcessorRemovals();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorHits() {
        return metricSrc.entryProcessorHits();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getEntryProcessorHitPercentage() {
        if (!metricSrc.enabled())
            return 0;

        long hits = metricSrc.entryProcessorHits();

        long totalInvocations = getEntryProcessorInvocations();

        if (hits == 0)
            return 0;

        return (float)hits / totalInvocations * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorMisses() {
        return metricSrc.entryProcessorMisses();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getEntryProcessorMissPercentage() {
        if (!metricSrc.enabled())
            return 0;

        long misses = metricSrc.entryProcessorMisses();

        long totalInvocations = getEntryProcessorInvocations();

        if (misses == 0)
            return 0;

        return (float)misses / totalInvocations * 100.0f;
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getEntryProcessorAverageInvocationTime() {
        if (!metricSrc.enabled())
            return 0;

        long totalInvokes = getEntryProcessorInvocations();

        long timeNanos = metricSrc.entryProcessorInvokeTimeNanos();

        if (timeNanos == 0 || totalInvokes == 0)
            return 0;

        return (1f * timeNanos) / totalInvokes / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMinInvocationTime() {
        if (!metricSrc.enabled())
            return 0;

        return (1f * metricSrc.entryProcessorMinInvocationTime()) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMaxInvocationTime() {
        if (!metricSrc.enabled())
            return 0;

        return (1f * metricSrc.entryProcessorMaxInvocationTime()) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return metricSrc.removals();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return metricSrc.evictions();
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getAverageGetTime() {
        if (!metricSrc.enabled())
            return 0;

        long timeNanos = metricSrc.getTimeTotal();

        long readsCnt = metricSrc.reads();

        if (timeNanos == 0 || readsCnt == 0)
            return 0;

        return ((1f * timeNanos) / readsCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getAveragePutTime() {
        if (!metricSrc.enabled())
            return 0;

        long timeNanos = metricSrc.putTimeTotal();

        long putsCnt = metricSrc.writes();

        if (timeNanos == 0 || putsCnt == 0)
            return 0;

        return ((1f * timeNanos) / putsCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * @deprecated Should be removed in Apache Ignite 3.0. Metrics aggregation is out of scope and shuld be done with
     * external systems.
     */
    @Deprecated
    @Override public float getAverageRemoveTime() {
        if (!metricSrc.enabled())
            return 0;

        long timeNanos = metricSrc.removeTimeTotal();

        long removesCnt = metricSrc.removals();

        if (timeNanos == 0 || removesCnt == 0)
            return 0;

        return ((1f * timeNanos) / removesCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        metricSrc.onRead(isHit);
    }

    /**
     * Set callback for tx key collisions detection.
     *
     * @param coll Key collisions info holder.
     */
    //TODO: Move to metric source
    public void keyCollisionsInfo(Supplier<List<Map.Entry</* Colliding keys. */ GridCacheMapEntry, /* Collisions queue size. */ Integer>>> coll) {
/*
        txKeyCollisionInfo = coll;

        if (delegate != null)
            delegate.keyCollisionsInfo(coll);
*/
    }

    /** Callback representing current key collisions state.
     *
     * @return Key collisions info holder.
     */
    //TODO: Move to metric source
    public @Nullable Supplier<List<Map.Entry<GridCacheMapEntry, Integer>>> keyCollisionsInfo() {
        return txKeyCollisionInfo;
    }

    /** {@inheritDoc} */
    //TODO: Move to metric source
    @Override public String getTxKeyCollisions() {
/*
        SB sb = null;

        Supplier<List<Map.Entry<GridCacheMapEntry, Integer>>> collInfo = keyCollisionsInfo();

        if (collInfo != null) {
            List<Map.Entry<GridCacheMapEntry, Integer>> result = collInfo.get();

            if (!F.isEmpty(result)) {
                sb = new SB();

                for (Map.Entry<GridCacheMapEntry, Integer> info : result) {
                    if (sb.length() > 0)
                        sb.a(U.nl());
                    sb.a("key=");
                    sb.a(info.getKey().key());
                    sb.a(", queueSize=");
                    sb.a(info.getValue());
                }
            }
        }

        return sb != null ? sb.toString() : "";
*/
        return null; // ^^^
    }

    /**
     * Cache invocations caused update callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeUpdate(boolean isHit) {
        metricSrc.onInvokeUpdate(isHit);
    }

    /**
     * Cache invocations caused removal callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeRemove(boolean isHit) {
        metricSrc.onInvokeRemove(isHit);
    }

    /**
     * Read-only cache invocations.
     *
     * @param isHit Hit or miss flag.
     */
    public void onReadOnlyInvoke(boolean isHit) {
        metricSrc.onReadOnlyInvoke(isHit);
    }

    /**
     * Increments invoke operation time nanos.
     *
     * @param duration Duration.
     */
    public void addInvokeTimeNanos(long duration) {
        metricSrc.addInvokeTimeNanos(duration);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        metricSrc.onWrite();
    }

    /**
     * Cache remove callback.
     */
    public void onRemove() {
        metricSrc.onRemove();
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        metricSrc.onEvict();
    }

    /**
     * Transaction commit callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxCommit(long duration) {
        //TODO: Add to metric source update of new commitTime metric
        //commitTime.value(duration);
        metricSrc.onTxCommit(duration);
    }

    /**
     * Transaction rollback callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxRollback(long duration) {
        //TODO: Add to metric source update of new rollbackTime metric
        //rollbackTime.value(duration);
        metricSrc.onTxRollback(duration);
    }

    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        metricSrc.addGetTimeNanos(duration);
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        metricSrc.addPutTimeNanos(duration);
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        metricSrc.addRemoveTimeTotal(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        metricSrc.addRemoveAndGetTimeNanos(duration);
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        metricSrc.addPutAndGetTimeNanos(duration);
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated Not a metric. Should be in system view.
     */
    @Deprecated
    @Override public String getKeyType() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null ? ccfg.getKeyType().getName() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated Not a metric. Should be in system view.
     */
    @Deprecated
    @Override public String getValueType() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null ? ccfg.getValueType().getName() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated Not a metric. Should be in system view.
     */
    @Deprecated
    @Override public boolean isReadThrough() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null && ccfg.isReadThrough();
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated Not a metric. Should be in system view.
     */
    @Deprecated
    @Override public boolean isWriteThrough() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null && ccfg.isWriteThrough();
    }

    /**
     * Checks whether cache topology is valid for operations.
     *
     * @param read {@code True} if validating read operations, {@code false} if validating write.
     * @return Valid ot not.
     */
    private boolean isValidForOperation(boolean read) {
        if (cctx.isLocal())
            return true;

        try {
            GridDhtTopologyFuture fut = cctx.shared().exchange().lastFinishedFuture();

            return (fut != null && fut.validateCache(cctx, false, read, null, null) == null);
        }
        catch (Exception ignored) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForReading() {
        return isValidForOperation(true);
    }

    /** {@inheritDoc} */
    @Override public boolean isValidForWriting() {
        return isValidForOperation(false);
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null && ccfg.isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return metricSrc.enabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        CacheConfiguration<?, ?> ccfg = cctx.config();

        return ccfg != null && ccfg.isManagementEnabled();
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return metricSrc.totalPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return metricSrc.rebalancingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return metricSrc.rebalancedKeys();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return metricSrc.estimatedRebalancingKeys();
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated Transitive. Can be calculated in external system.
     */
    @Deprecated
    @Override public long getKeysToRebalanceLeft() {
        return metricSrc.keysToRebalanceLeft();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return metricSrc.rebalancingKeysRate();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return metricSrc.rebalancingBytesRate();
    }

    /**
     * Clear rebalance counters.
     */
    public void clearRebalanceCounters() {
        metricSrc.resetRebalanceCounters();
    }

    /**
     *
     */
    public void startRebalance(long delay) {
        metricSrc.startRebalance(delay);
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return getEstimatedRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return getRebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        return metricSrc.estimatedRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return metricSrc.rebalancingStartTime();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return metricSrc.rebalanceClearingPartitionsLeft();
    }

    /**
     * Sets clearing partitions number.
     * @param partitions Partitions number.
     */
    //TODO: May be unused in future sew below
    public void rebalanceClearingPartitions(int partitions) {
        metricSrc.rebalanceClearingPartitions(partitions);
    }

    /** */
    //TODO: Move to metric source
    public long evictingPartitionsLeft() {
        return evictingPartitions.value();
    }

    /** */
    //TODO: Move to metric source
    public void incrementRebalanceClearingPartitions() {
      //  rebalanceClearingPartitions.increment();
    }

    /** */
    //TODO: Move to metric source
    public void decrementRebalanceClearingPartitions() {
      //  rebalanceClearingPartitions.decrement();
    }

    /** */
    //TODO: Move to metric source
    public void incrementEvictingPartitions() {
        evictingPartitions.increment();
    }

    /** */
    //TODO: Move to metric source
    public void decrementEvictingPartitions() {
        evictingPartitions.decrement();
    }

    /**
     * First rebalance supply message callback.
     * @param keysCnt Estimated number of keys.
     */
    public void onRebalancingKeysCountEstimateReceived(Long keysCnt) {
        metricSrc.onRebalancingKeysCountEstimateReceived(keysCnt);
    }

    /**
     * Rebalance entry store callback.
     */
    public void onRebalanceKeyReceived() {
        metricSrc.onRebalanceKeyReceived();
    }

    /**
     * Rebalance supply message callback.
     *
     * @param batchSize Batch size in bytes.
     */
    public void onRebalanceBatchReceived(long batchSize) {
        metricSrc.onRebalanceBatchReceived(batchSize);
    }

    /** Calculates statistics for cache entries. */
    public CacheMetricSource.EntriesStatMetrics getEntriesStat() {
        return metricSrc.entriesStat();
    }

    /** {@inheritDoc} */
    //TODO: Move to metric source
    @Override public boolean isIndexRebuildInProgress() {
/*
        IgniteInternalFuture fut = cctx.shared().kernalContext().query().indexRebuildFuture(cctx.cacheId());

        return fut != null && !fut.isDone();
*/
        return false; // ^^^
    }

    /** {@inheritDoc} */
    //TODO: Move to metric source
    @Override public long getIndexRebuildKeysProcessed() {
        return idxRebuildKeyProcessed.value();
    }

    /** Reset metric - number of keys processed during index rebuilding. */
    //TODO: Move to metric source
    public void resetIndexRebuildKeyProcessed() {
        idxRebuildKeyProcessed.reset();
    }

    /**
     * Increase number of keys processed during index rebuilding.
     *
     * @param val Number of processed keys.
     */
    //TODO: Move to metric source
    public void addIndexRebuildKeyProcessed(long val) {
        idxRebuildKeyProcessed.add(val);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsImpl.class, this);
    }
}
