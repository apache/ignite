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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongGauge;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Adapter for cache metrics.
 */
public class CacheMetricsImpl implements CacheMetrics {
    /** Rebalance rate interval. */
    private static final int REBALANCE_RATE_INTERVAL = IgniteSystemProperties.getInteger(
        IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL, 60000);

    /** Onheap peek modes. */
    private static final CachePeekMode[] ONHEAP_PEEK_MODES = new CachePeekMode[] {
        CachePeekMode.ONHEAP, CachePeekMode.PRIMARY, CachePeekMode.BACKUP, CachePeekMode.NEAR};

    /** */
    private static final long NANOS_IN_MICROSECOND = 1000L;

    /**
     * Cache metrics registry name first part.
     * Full name will contain {@link CacheConfiguration#getName()} also.
     * {@code "cache.sys-cache"}, for example.
     */
    public static final String CACHE_METRICS = "cache";

    /** Histogram buckets for duration get, put, remove, commit, rollback operations in nanoseconds. */
    public static final long[] HISTOGRAM_BUCKETS = new long[] {
        NANOSECONDS.convert(1, MILLISECONDS),
        NANOSECONDS.convert(10, MILLISECONDS),
        NANOSECONDS.convert(100, MILLISECONDS),
        NANOSECONDS.convert(250, MILLISECONDS),
        NANOSECONDS.convert(1000, MILLISECONDS)
    };

    /** Number of reads. */
    private final AtomicLongMetric reads;

    /** Number of invocations caused update. */
    private final AtomicLongMetric entryProcessorPuts;

    /** Number of invocations caused removal. */
    private final AtomicLongMetric entryProcessorRemovals;

    /** Number of invocations caused update. */
    private final AtomicLongMetric entryProcessorReadOnlyInvocations;

    /** Entry processor invoke time taken nanos. */
    private final AtomicLongMetric entryProcessorInvokeTimeNanos;

    /** So far, the minimum time to execute cache invokes. */
    private final AtomicLongMetric entryProcessorMinInvocationTime;

    /** So far, the maximum time to execute cache invokes. */
    private final AtomicLongMetric entryProcessorMaxInvocationTime;

    /** Number of entry processor invokes on keys, which exist in cache. */
    private final AtomicLongMetric entryProcessorHits;

    /** Number of entry processor invokes on keys, which don't exist in cache. */
    private final AtomicLongMetric entryProcessorMisses;

    /** Number of writes. */
    private final AtomicLongMetric writes;

    /** Number of hits. */
    private final AtomicLongMetric hits;

    /** Number of misses. */
    private final AtomicLongMetric misses;

    /** Number of transaction commits. */
    private final AtomicLongMetric txCommits;

    /** Number of transaction rollbacks. */
    private final AtomicLongMetric txRollbacks;

    /** Number of evictions. */
    private final AtomicLongMetric evictCnt;

    /** Number of removed entries. */
    private final AtomicLongMetric rmCnt;

    /** Total put time taken nanos. */
    private final AtomicLongMetric putTimeTotal;

    /** Total get time taken nanos. */
    private final AtomicLongMetric getTimeTotal;

    /** Total remove time taken nanos. */
    private final AtomicLongMetric rmvTimeTotal;

    /** Total commit transaction time taken nanos. */
    private final AtomicLongMetric commitTimeTotal;

    /** Total rollback transaction time taken nanos. */
    private final AtomicLongMetric rollbackTimeTotal;

    /** Number of reads from off-heap memory. */
    private final AtomicLongMetric offHeapGets;

    /** Number of writes to off-heap memory. */
    private final AtomicLongMetric offHeapPuts;

    /** Number of removed entries from off-heap memory. */
    private final AtomicLongMetric offHeapRemoves;

    /** Number of evictions from off-heap memory. */
    private final AtomicLongMetric offHeapEvicts;

    /** Number of off-heap hits. */
    private final AtomicLongMetric offHeapHits;

    /** Number of off-heap misses. */
    private final AtomicLongMetric offHeapMisses;

    /** Rebalanced keys count. */
    private final AtomicLongMetric rebalancedKeys;

    /** Total rebalanced bytes count. */
    private final AtomicLongMetric totalRebalancedBytes;

    /** Rebalanced start time. */
    private final AtomicLongMetric rebalanceStartTime;

    /** Estimated rebalancing keys count. */
    private final AtomicLongMetric estimatedRebalancingKeys;

    /** Rebalancing rate in keys. */
    private final HitRateMetric rebalancingKeysRate;

    /** Rebalancing rate in bytes. */
    private final HitRateMetric rebalancingBytesRate;

    /** Number of currently clearing partitions for rebalancing. */
    private final AtomicLongMetric rebalanceClearingPartitions;

    /** Get time. */
    private final HistogramMetricImpl getTime;

    /** Put time. */
    private final HistogramMetricImpl putTime;

    /** Remove time. */
    private final HistogramMetricImpl rmvTime;

    /** Commit time. */
    private final HistogramMetricImpl commitTime;

    /** Rollback time. */
    private final HistogramMetricImpl rollbackTime;

    /** Cache metrics. */
    @GridToStringExclude
    private transient CacheMetricsImpl delegate;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /** DHT context. */
    private GridCacheContext<?, ?> dhtCtx;

    /** Write-behind store, if configured. */
    private GridCacheWriteBehindStore store;

    /** Tx collisions info. */
    private volatile Supplier<List<Map.Entry</* Colliding keys. */ GridCacheMapEntry, /* Collisions queue size. */ Integer>>> txKeyCollisionInfo;

    /** Offheap entries count. */
    private final LongGauge offHeapEntriesCnt;

    /** Offheap primary entries count. */
    private final LongGauge offHeapPrimaryEntriesCnt;

    /** Offheap backup entries count. */
    private final LongGauge offHeapBackupEntriesCnt;

    /** Onheap entries count. */
    private final LongGauge heapEntriesCnt;

    /** Cache size. */
    private final LongGauge cacheSize;

    /**
     * Creates cache metrics.
     *
     * @param cctx Cache context.
     */
    public CacheMetricsImpl(GridCacheContext<?, ?> cctx) {
        this(cctx, false);
    }

    /**
     * Creates cache metrics.
     *
     * @param cctx Cache context.
     * @param isNear Is near flag.
     */
    public CacheMetricsImpl(GridCacheContext<?, ?> cctx, boolean isNear) {
        assert cctx != null;

        this.cctx = cctx;

        if (cctx.isNear())
            dhtCtx = cctx.near().dht().context();

        if (cctx.store().store() instanceof GridCacheWriteBehindStore)
            store = (GridCacheWriteBehindStore)cctx.store().store();

        delegate = null;

        MetricRegistry mreg = cctx.kernalContext().metric().registry(cacheMetricsRegistryName(cctx.name(), isNear));

        reads = mreg.longMetric("CacheGets",
            "The total number of gets to the cache.");

        entryProcessorPuts = mreg.longMetric("EntryProcessorPuts",
            "The total number of cache invocations, caused update.");

        entryProcessorRemovals = mreg.longMetric("EntryProcessorRemovals",
            "The total number of cache invocations, caused removals.");

        entryProcessorReadOnlyInvocations = mreg.longMetric("EntryProcessorReadOnlyInvocations",
            "The total number of cache invocations, caused no updates.");

        entryProcessorInvokeTimeNanos = mreg.longMetric("EntryProcessorInvokeTimeNanos",
            "The total time of cache invocations, in nanoseconds.");

        entryProcessorMinInvocationTime = mreg.longMetric("EntryProcessorMinInvocationTime",
            "So far, the minimum time to execute cache invokes.");

        entryProcessorMaxInvocationTime = mreg.longMetric("EntryProcessorMaxInvocationTime",
            "So far, the maximum time to execute cache invokes.");

        entryProcessorHits = mreg.longMetric("EntryProcessorHits",
            "The total number of invocations on keys, which exist in cache.");

        entryProcessorMisses = mreg.longMetric("EntryProcessorMisses",
            "The total number of invocations on keys, which don't exist in cache.");

        writes = mreg.longMetric("CachePuts",
            "The total number of puts to the cache.");

        hits = mreg.longMetric("CacheHits",
            "The number of get requests that were satisfied by the cache.");

        misses = mreg.longMetric("CacheMisses",
            "A miss is a get request that is not satisfied.");

        txCommits = mreg.longMetric("CacheTxCommits",
            "Total number of transaction commits.");

        txRollbacks = mreg.longMetric("CacheTxRollbacks",
            "Total number of transaction rollbacks.");

        evictCnt = mreg.longMetric("CacheEvictions",
            "The total number of evictions from the cache.");

        rmCnt = mreg.longMetric("CacheRemovals", "The total number of removals from the cache.");

        putTimeTotal = mreg.longMetric("PutTimeTotal",
            "The total time of cache puts, in nanoseconds.");

        getTimeTotal = mreg.longMetric("GetTimeTotal",
            "The total time of cache gets, in nanoseconds.");

        rmvTimeTotal = mreg.longMetric("RemoveTimeTotal",
            "The total time of cache removal, in nanoseconds.");

        commitTimeTotal = mreg.longMetric("CommitTimeTotal",
            "The total time of commit, in nanoseconds.");

        rollbackTimeTotal = mreg.longMetric("RollbackTimeTotal",
            "The total time of rollback, in nanoseconds.");

        offHeapGets = mreg.longMetric("OffHeapGets",
            "The total number of get requests to the off-heap memory.");

        offHeapPuts = mreg.longMetric("OffHeapPuts",
            "The total number of put requests to the off-heap memory.");

        offHeapRemoves = mreg.longMetric("OffHeapRemovals",
            "The total number of removals from the off-heap memory.");

        offHeapEvicts = mreg.longMetric("OffHeapEvictions",
            "The total number of evictions from the off-heap memory.");

        offHeapHits = mreg.longMetric("OffHeapHits",
            "The number of get requests that were satisfied by the off-heap memory.");

        offHeapMisses = mreg.longMetric("OffHeapMisses",
            "A miss is a get request that is not satisfied by off-heap memory.");

        rebalancedKeys = mreg.longMetric("RebalancedKeys",
            "Number of already rebalanced keys.");

        totalRebalancedBytes = mreg.longMetric("TotalRebalancedBytes",
            "Number of already rebalanced bytes.");

        rebalanceStartTime = mreg.longMetric("RebalanceStartTime",
            "Rebalance start time");

        rebalanceStartTime.value(-1);

        estimatedRebalancingKeys = mreg.longMetric("EstimatedRebalancingKeys",
            "Number estimated to rebalance keys.");

        rebalancingKeysRate = mreg.hitRateMetric("RebalancingKeysRate",
            "Estimated rebalancing speed in keys",
            REBALANCE_RATE_INTERVAL,
            20);

        rebalancingBytesRate = mreg.hitRateMetric("RebalancingBytesRate",
            "Estimated rebalancing speed in bytes",
            REBALANCE_RATE_INTERVAL,
            20);

        rebalanceClearingPartitions = mreg.longMetric("RebalanceClearingPartitionsLeft",
            "Number of partitions need to be cleared before actual rebalance start.");

        mreg.register("IsIndexRebuildInProgress", () -> {
            IgniteInternalFuture fut = cctx.shared().kernalContext().query().indexRebuildFuture(cctx.cacheId());

            return fut != null && !fut.isDone();
        }, "True if index rebuild is in progress.");

        getTime = mreg.histogram("GetTime", HISTOGRAM_BUCKETS, "Get time in nanoseconds.");

        putTime = mreg.histogram("PutTime", HISTOGRAM_BUCKETS, "Put time in nanoseconds.");

        rmvTime = mreg.histogram("RemoveTime", HISTOGRAM_BUCKETS, "Remove time in nanoseconds.");

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
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(CacheMetricsImpl delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return cctx.name();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapGets() {
        return offHeapGets.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return offHeapPuts.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return offHeapRemoves.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return offHeapEvicts.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return offHeapHits.value();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        long hits0 = offHeapHits.value();

        long gets0 = offHeapGets.value();

        if (hits0 == 0)
            return 0;

        return (float)hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return offHeapMisses.value();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        long misses0 = offHeapMisses.value();

        long reads0 = offHeapGets.value();

        if (misses0 == 0)
            return 0;

        return (float)misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return offHeapEntriesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return heapEntriesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return offHeapPrimaryEntriesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return offHeapBackupEntriesCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        GridCacheAdapter<?, ?> cache = cctx.cache();

        return cache != null ? cache.offHeapAllocatedSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return getEntriesStat().size();
    }

    /** {@inheritDoc} */
    @Override public long getCacheSize() {
        return cacheSize.value();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return getEntriesStat().keySize();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return getEntriesStat().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxThreadMapSize() {
        return cctx.tm().threadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxXidMapSize() {
        return cctx.tm().idMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxPrepareQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxCommittedVersionsSize() {
        return cctx.tm().completedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxRolledbackVersionsSize() {
        return cctx.tm().completedVersionsSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtThreadMapSize() {
        return cctx.tm().threadMapSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().idMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommittedVersionsSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().completedVersionsSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtRolledbackVersionsSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().completedVersionsSize() : -1;
    }

    /** {@inheritDoc} */
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
        return store != null ? store.getWriteBehindBufferSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxCommitTime() {
        long timeNanos = commitTimeTotal.value();

        long commitsCnt = txCommits.value();

        if (timeNanos == 0 || commitsCnt == 0)
            return 0;

        return ((1f * timeNanos) / commitsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        long timeNanos = rollbackTimeTotal.value();

        long rollbacksCnt = txRollbacks.value();

        if (timeNanos == 0 || rollbacksCnt == 0)
            return 0;

        return ((1f * timeNanos) / rollbacksCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits.value();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks.value();
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        reads.reset();
        writes.reset();
        rmCnt.reset();
        hits.reset();
        misses.reset();
        evictCnt.reset();
        txCommits.reset();
        txRollbacks.reset();
        putTimeTotal.reset();
        rmvTimeTotal.reset();
        getTimeTotal.reset();
        commitTimeTotal.reset();
        rollbackTimeTotal.reset();

        entryProcessorPuts.reset();
        entryProcessorRemovals.reset();
        entryProcessorReadOnlyInvocations.reset();
        entryProcessorMisses.reset();
        entryProcessorHits.reset();
        entryProcessorInvokeTimeNanos.reset();
        entryProcessorMaxInvocationTime.reset();
        entryProcessorMinInvocationTime.reset();

        offHeapGets.reset();
        offHeapPuts.reset();
        offHeapRemoves.reset();
        offHeapHits.reset();
        offHeapMisses.reset();
        offHeapEvicts.reset();

        getTime.reset();
        putTime.reset();
        rmvTime.reset();
        commitTime.reset();
        rollbackTime.reset();

        clearRebalanceCounters();

        if (delegate != null)
            delegate.clear();

        txKeyCollisionInfo = null;
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits.value();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        long hits0 = hits.value();

        long gets0 = reads.value();

        if (hits0 == 0)
            return 0;

        return (float)hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses.value();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        long misses0 = misses.value();

        long reads0 = reads.value();

        if (misses0 == 0)
            return 0;

        return (float)misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads.value();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return writes.value();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorPuts() {
        return entryProcessorPuts.value();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorRemovals() {
        return entryProcessorRemovals.value();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorReadOnlyInvocations() {
        return entryProcessorReadOnlyInvocations.value();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorInvocations() {
        return entryProcessorReadOnlyInvocations.value() + entryProcessorPuts.value() + entryProcessorRemovals.value();
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorHits() {
        return entryProcessorHits.value();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorHitPercentage() {
        long hits = entryProcessorHits.value();

        long totalInvocations = getEntryProcessorInvocations();

        if (hits == 0)
            return 0;

        return (float)hits / totalInvocations * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getEntryProcessorMisses() {
        return entryProcessorMisses.value();
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMissPercentage() {
        long misses = entryProcessorMisses.value();

        long totalInvocations = getEntryProcessorInvocations();

        if (misses == 0)
            return 0;

        return (float)misses / totalInvocations * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorAverageInvocationTime() {
        long totalInvokes = getEntryProcessorInvocations();

        long timeNanos = entryProcessorInvokeTimeNanos.value();

        if (timeNanos == 0 || totalInvokes == 0)
            return 0;

        return (1f * timeNanos) / totalInvokes / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMinInvocationTime() {
        return (1f * entryProcessorMinInvocationTime.value()) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getEntryProcessorMaxInvocationTime() {
        return (1f * entryProcessorMaxInvocationTime.value()) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return rmCnt.value();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evictCnt.value();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        long timeNanos = getTimeTotal.value();

        long readsCnt = reads.value();

        if (timeNanos == 0 || readsCnt == 0)
            return 0;

        return ((1f * timeNanos) / readsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        long timeNanos = putTimeTotal.value();

        long putsCnt = writes.value();

        if (timeNanos == 0 || putsCnt == 0)
            return 0;

        return ((1f * timeNanos) / putsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        long timeNanos = rmvTimeTotal.value();

        long removesCnt = rmCnt.value();

        if (timeNanos == 0 || removesCnt == 0)
            return 0;

        return ((1f * timeNanos) / removesCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        reads.increment();

        if (isHit)
            hits.increment();
        else
            misses.increment();

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /** Set callback for tx key collisions detection.
     *
     * @param coll Key collisions info holder.
     */
    public void keyCollisionsInfo(Supplier<List<Map.Entry</* Colliding keys. */ GridCacheMapEntry, /* Collisions queue size. */ Integer>>> coll) {
        txKeyCollisionInfo = coll;

        if (delegate != null)
            delegate.keyCollisionsInfo(coll);
    }

    /** Callback representing current key collisions state.
     *
     * @return Key collisions info holder.
     */
    public @Nullable Supplier<List<Map.Entry<GridCacheMapEntry, Integer>>> keyCollisionsInfo() {
        return txKeyCollisionInfo;
    }

    /** {@inheritDoc} */
    @Override public String getTxKeyCollisions() {
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
    }

    /**
     * Cache invocations caused update callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeUpdate(boolean isHit) {
        entryProcessorPuts.increment();

        if (isHit)
            entryProcessorHits.increment();
        else
            entryProcessorMisses.increment();

        if (delegate != null)
            delegate.onInvokeUpdate(isHit);
    }

    /**
     * Cache invocations caused removal callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeRemove(boolean isHit) {
        entryProcessorRemovals.increment();

        if (isHit)
            entryProcessorHits.increment();
        else
            entryProcessorMisses.increment();

        if (delegate != null)
            delegate.onInvokeRemove(isHit);
    }

    /**
     * Read-only cache invocations.
     *
     * @param isHit Hit or miss flag.
     */
    public void onReadOnlyInvoke(boolean isHit) {
        entryProcessorReadOnlyInvocations.increment();

        if (isHit)
            entryProcessorHits.increment();
        else
            entryProcessorMisses.increment();

        if (delegate != null)
            delegate.onReadOnlyInvoke(isHit);
    }

    /**
     * Increments invoke operation time nanos.
     *
     * @param duration Duration.
     */
    public void addInvokeTimeNanos(long duration) {
        entryProcessorInvokeTimeNanos.add(duration);

        recalculateInvokeMinTimeNanos(duration);

        recalculateInvokeMaxTimeNanos(duration);

        if (delegate != null)
            delegate.addInvokeTimeNanos(duration);

    }

    /**
     * Recalculates invoke operation minimum time nanos.
     *
     * @param duration Duration.
     */
    private void recalculateInvokeMinTimeNanos(long duration) {
        long minTime = entryProcessorMinInvocationTime.value();

        while (minTime > duration || minTime == 0) {
            if (MetricUtils.compareAndSet(entryProcessorMinInvocationTime, minTime, duration))
                break;
            else
                minTime = entryProcessorMinInvocationTime.value();
        }
    }

    /**
     * Recalculates invoke operation maximum time nanos.
     *
     * @param duration Duration.
     */
    private void recalculateInvokeMaxTimeNanos(long duration) {
        long maxTime = entryProcessorMaxInvocationTime.value();

        while (maxTime < duration) {
            if (MetricUtils.compareAndSet(entryProcessorMaxInvocationTime, maxTime, duration))
                break;
            else
                maxTime = entryProcessorMaxInvocationTime.value();
        }
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writes.increment();

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Cache remove callback.
     */
    public void onRemove() {
        rmCnt.increment();

        if (delegate != null)
            delegate.onRemove();
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        evictCnt.increment();

        if (delegate != null)
            delegate.onEvict();
    }

    /**
     * Transaction commit callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxCommit(long duration) {
        txCommits.increment();
        commitTimeTotal.add(duration);

        commitTime.value(duration);

        if (delegate != null)
            delegate.onTxCommit(duration);
    }

    /**
     * Transaction rollback callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxRollback(long duration) {
        txRollbacks.increment();
        rollbackTimeTotal.add(duration);

        rollbackTime.value(duration);

        if (delegate != null)
            delegate.onTxRollback(duration);
    }

    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        getTimeTotal.add(duration);

        getTime.value(duration);

        if (delegate != null)
            delegate.addGetTimeNanos(duration);
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        putTimeTotal.add(duration);

        putTime.value(duration);

        if (delegate != null)
            delegate.addPutTimeNanos(duration);
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        rmvTimeTotal.add(duration);

        rmvTime.value(duration);

        if (delegate != null)
            delegate.addRemoveTimeNanos(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        rmvTimeTotal.add(duration);
        getTimeTotal.add(duration);

        if (delegate != null)
            delegate.addRemoveAndGetTimeNanos(duration);
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        putTimeTotal.add(duration);
        getTimeTotal.add(duration);

        if (delegate != null)
            delegate.addPutAndGetTimeNanos(duration);
    }

    /** {@inheritDoc} */
    @Override public String getKeyType() {
        CacheConfiguration ccfg = cctx.config();

        return ccfg != null ? ccfg.getKeyType().getName() : null;
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        CacheConfiguration ccfg = cctx.config();

        return ccfg != null ? ccfg.getValueType().getName() : null;
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        CacheConfiguration ccfg = cctx.config();

        return ccfg != null && ccfg.isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        CacheConfiguration ccfg = cctx.config();

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
        CacheConfiguration ccfg = cctx.config();

        return ccfg != null && ccfg.isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cctx.statisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        CacheConfiguration ccfg = cctx.config();

        return ccfg != null && ccfg.isManagementEnabled();
    }

    /**
     * Calculates entries count/partitions count metrics using one iteration over local partitions for all metrics
     */
    public EntriesStatMetrics getEntriesStat() {
        int owningPartCnt = 0;
        int movingPartCnt = 0;
        long offHeapEntriesCnt = 0L;
        long offHeapPrimaryEntriesCnt = 0L;
        long offHeapBackupEntriesCnt = 0L;
        long heapEntriesCnt = 0L;
        int size = 0;
        long sizeLong = 0L;
        boolean isEmpty;

        try {
            AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

            if (AffinityTopologyVersion.NONE.equals(topVer))
                return unknownEntriesStat();

            final GridCacheAdapter<?, ?> cache = cctx.cache();

            if (cache != null) {
                offHeapEntriesCnt = cache.offHeapEntriesCount();

                size = cache.localSize(null);
                sizeLong = cache.localSizeLong(null);
            }

            if (cctx.isLocal()) {
                if (cache != null) {
                    offHeapPrimaryEntriesCnt = offHeapEntriesCnt;

                    heapEntriesCnt = cache.sizeLong();
                }
            }
            else {
                IntSet primaries = ImmutableIntSet.wrap(cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer));
                IntSet backups = ImmutableIntSet.wrap(cctx.affinity().backupPartitions(cctx.localNodeId(), topVer));

                if (cctx.isNear() && cache != null)
                    heapEntriesCnt = cache.nearSize();

                for (GridDhtLocalPartition part : cctx.topology().currentLocalPartitions()) {
                    // Partitions count.
                    GridDhtPartitionState partState = part.state();

                    if (partState == GridDhtPartitionState.OWNING)
                        owningPartCnt++;

                    if (partState == GridDhtPartitionState.MOVING)
                        movingPartCnt++;

                    // Offheap entries count
                    if (cache == null)
                        continue;

                    long cacheSize = part.dataStore().cacheSize(cctx.cacheId());

                    if (primaries.contains(part.id()))
                        offHeapPrimaryEntriesCnt += cacheSize;
                    else if (backups.contains(part.id()))
                        offHeapBackupEntriesCnt += cacheSize;

                    heapEntriesCnt += part.publicSize(cctx.cacheId());
                }
            }
        }
        catch (Exception e) {
            return unknownEntriesStat();
        }

        isEmpty = (offHeapEntriesCnt == 0);

        EntriesStatMetrics stat = new EntriesStatMetrics();

        stat.offHeapEntriesCount(offHeapEntriesCnt);
        stat.offHeapPrimaryEntriesCount(offHeapPrimaryEntriesCnt);
        stat.offHeapBackupEntriesCount(offHeapBackupEntriesCnt);
        stat.heapEntriesCount(heapEntriesCnt);
        stat.size(size);
        stat.cacheSize(sizeLong);
        stat.keySize(size);
        stat.isEmpty(isEmpty);
        stat.totalPartitionsCount(owningPartCnt + movingPartCnt);
        stat.rebalancingPartitionsCount(movingPartCnt);

        return stat;
    }

    /** @return Instance of {@link EntriesStatMetrics} with default values in case of unknown metrics. */
    private EntriesStatMetrics unknownEntriesStat() {
        EntriesStatMetrics stat = new EntriesStatMetrics();

        stat.offHeapEntriesCount(-1L);
        stat.offHeapPrimaryEntriesCount(-1L);
        stat.offHeapBackupEntriesCount(-1L);
        stat.heapEntriesCount(-1L);
        stat.size(-1);
        stat.cacheSize(-1L);
        stat.keySize(-1);
        stat.isEmpty(false);
        stat.totalPartitionsCount(-1);
        stat.rebalancingPartitionsCount(0);

        return stat;
    }

    /** {@inheritDoc} */
    @Override public int getTotalPartitionsCount() {
        return getEntriesStat().totalPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public int getRebalancingPartitionsCount() {
        return getEntriesStat().rebalancingPartitionsCount();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancedKeys() {
        return rebalancedKeys.value();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return estimatedRebalancingKeys.value();
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return Math.max(0, estimatedRebalancingKeys.value() - rebalancedKeys.value());
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return rebalancingKeysRate.value();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return rebalancingBytesRate.value();
    }

    /**
     * Clear rebalance counters.
     */
    public void clearRebalanceCounters() {
        estimatedRebalancingKeys.reset();

        rebalancedKeys.reset();

        totalRebalancedBytes.reset();

        rebalancingBytesRate.reset();

        rebalancingKeysRate.reset();

        rebalanceStartTime.value(-1L);
    }

    /**
     *
     */
    public void startRebalance(long delay) {
        rebalanceStartTime.value(delay + U.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return getEstimatedRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return rebalanceStartTime.value();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        long rate = rebalancingKeysRate.value();

        return rate <= 0 ? -1L :
            ((getKeysToRebalanceLeft() / rate) * REBALANCE_RATE_INTERVAL) + U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return rebalanceStartTime.value();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return rebalanceClearingPartitions.value();
    }

    /**
     * Sets clearing partitions number.
     * @param partitions Partitions number.
     */
    public void rebalanceClearingPartitions(int partitions) {
        rebalanceClearingPartitions.value(partitions);
    }

    /**
     * First rebalance supply message callback.
     * @param keysCnt Estimated number of keys.
     */
    public void onRebalancingKeysCountEstimateReceived(Long keysCnt) {
        if (keysCnt == null)
            return;

        estimatedRebalancingKeys.add(keysCnt);
    }

    /**
     * Rebalance entry store callback.
     */
    public void onRebalanceKeyReceived() {
        rebalancedKeys.increment();

        rebalancingKeysRate.increment();
    }

    /**
     * Rebalance supply message callback.
     *
     * @param batchSize Batch size in bytes.
     */
    public void onRebalanceBatchReceived(long batchSize) {
        totalRebalancedBytes.add(batchSize);

        rebalancingBytesRate.add(batchSize);
    }

    /**
     * @return Total number of allocated pages.
     */
    public long getTotalAllocatedPages() {
        return 0;
    }

    /**
     * @return Total number of evicted pages.
     */
    public long getTotalEvictedPages() {
        return 0;
    }

    /**
     * Off-heap read callback.
     *
     * @param hit Hit or miss flag.
     */
    public void onOffHeapRead(boolean hit) {
        offHeapGets.increment();

        if (hit)
            offHeapHits.increment();
        else
            offHeapMisses.increment();

        if (delegate != null)
            delegate.onOffHeapRead(hit);
    }

    /**
     * Off-heap write callback.
     */
    public void onOffHeapWrite() {
        offHeapPuts.increment();

        if (delegate != null)
            delegate.onOffHeapWrite();
    }

    /**
     * Off-heap remove callback.
     */
    public void onOffHeapRemove() {
        offHeapRemoves.increment();

        if (delegate != null)
            delegate.onOffHeapRemove();
    }

    /**
     * Off-heap evict callback.
     */
    public void onOffHeapEvict() {
        offHeapEvicts.increment();

        if (delegate != null)
            delegate.onOffHeapEvict();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsImpl.class, this);
    }

    /**
     * Entries and partitions metrics holder class.
     */
    public static class EntriesStatMetrics {
        /** Total partitions count. */
        private int totalPartsCnt;

        /** Rebalancing partitions count. */
        private int rebalancingPartsCnt;

        /** Offheap entries count. */
        private long offHeapEntriesCnt;

        /** Offheap primary entries count. */
        private long offHeapPrimaryEntriesCnt;

        /** Offheap backup entries count. */
        private long offHeapBackupEntriesCnt;

        /** Onheap entries count. */
        private long heapEntriesCnt;

        /** Size. */
        private int size;

        /** Long size. */
        private long cacheSize;

        /** Key size. */
        private int keySize;

        /** Is empty. */
        private boolean isEmpty;

        /**
         * @return Total partitions count.
         */
        public int totalPartitionsCount() {
            return totalPartsCnt;
        }

        /**
         * @param totalPartsCnt Total partitions count.
         */
        public void totalPartitionsCount(int totalPartsCnt) {
            this.totalPartsCnt = totalPartsCnt;
        }

        /**
         * @return Rebalancing partitions count.
         */
        public int rebalancingPartitionsCount() {
            return rebalancingPartsCnt;
        }

        /**
         * @param rebalancingPartsCnt Rebalancing partitions count.
         */
        public void rebalancingPartitionsCount(int rebalancingPartsCnt) {
            this.rebalancingPartsCnt = rebalancingPartsCnt;
        }

        /**
         * @return Offheap entries count.
         */
        public long offHeapEntriesCount() {
            return offHeapEntriesCnt;
        }

        /**
         * @param offHeapEntriesCnt Offheap entries count.
         */
        public void offHeapEntriesCount(long offHeapEntriesCnt) {
            this.offHeapEntriesCnt = offHeapEntriesCnt;
        }

        /**
         * @return Offheap primary entries count.
         */
        public long offHeapPrimaryEntriesCount() {
            return offHeapPrimaryEntriesCnt;
        }

        /**
         * @param offHeapPrimaryEntriesCnt Offheap primary entries count.
         */
        public void offHeapPrimaryEntriesCount(long offHeapPrimaryEntriesCnt) {
            this.offHeapPrimaryEntriesCnt = offHeapPrimaryEntriesCnt;
        }

        /**
         * @return Offheap backup entries count.
         */
        public long offHeapBackupEntriesCount() {
            return offHeapBackupEntriesCnt;
        }

        /**
         * @param offHeapBackupEntriesCnt Offheap backup entries count.
         */
        public void offHeapBackupEntriesCount(long offHeapBackupEntriesCnt) {
            this.offHeapBackupEntriesCnt = offHeapBackupEntriesCnt;
        }

        /**
         * @return Heap entries count.
         */
        public long heapEntriesCount() {
            return heapEntriesCnt;
        }

        /**
         * @param heapEntriesCnt Onheap entries count.
         */
        public void heapEntriesCount(long heapEntriesCnt) {
            this.heapEntriesCnt = heapEntriesCnt;
        }

        /**
         * @return Size.
         */
        public int size() {
            return size;
        }

        /**
         * @param size Size.
         */
        public void size(int size) {
            this.size = size;
        }

        /**
         * @return Key size.
         */
        public int keySize() {
            return keySize;
        }

        /**
         * @param keySize Key size.
         */
        public void keySize(int keySize) {
            this.keySize = keySize;
        }

        /**
         * @return Long size.
         */
        public long cacheSize() {
            return cacheSize;
        }

        /**
         * @param cacheSize Size long.
         */
        public void cacheSize(long cacheSize) {
            this.cacheSize = cacheSize;
        }

        /**
         * @return Is empty.
         */
        public boolean isEmpty() {
            return isEmpty;
        }

        /**
         * @param isEmpty Is empty flag.
         */
        public void isEmpty(boolean isEmpty) {
            this.isEmpty = isEmpty;
        }
    }
}
