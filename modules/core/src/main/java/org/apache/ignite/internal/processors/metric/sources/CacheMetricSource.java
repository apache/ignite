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

package org.apache.ignite.internal.processors.metric.sources;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.util.collection.ImmutableIntSet;
import org.apache.ignite.internal.util.collection.IntSet;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Cache metric source.
 */
public class CacheMetricSource extends AbstractMetricSource<CacheMetricSource.Holder> {
    /**
     * Cache metrics registry name first part.
     * Full name will contain {@link CacheConfiguration#getName()} also.
     * {@code "cache.sys-cache"}, for example.
     */
    public static final String CACHE_METRICS = "cache";

    /** Rebalance rate interval. */
    private static final int REBALANCE_RATE_INTERVAL = IgniteSystemProperties.getInteger(
            IgniteSystemProperties.IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL, 60000);

    /** Histogram buckets for duration get, put, remove operations in nanoseconds. */
    private static final long[] HISTOGRAM_BUCKETS = new long[] {
            NANOSECONDS.convert(1, MILLISECONDS),
            NANOSECONDS.convert(10, MILLISECONDS),
            NANOSECONDS.convert(100, MILLISECONDS),
            NANOSECONDS.convert(250, MILLISECONDS),
            NANOSECONDS.convert(1000, MILLISECONDS)
    };

    /** Cache context. */
    private final GridCacheContext<?, ?> cctx;

    /** Near cache metrics if configured. */
    @Nullable private CacheMetricSource delegate;

    /** Write-behind store, if configured. */
    @Nullable private final GridCacheWriteBehindStore<?, ?> store;

    /**
     * Creates cache metric source.
     *
     * @param cctx Cache context.
     * @param near Near cache flag.
     */
    public CacheMetricSource(GridCacheContext<?, ?> cctx, boolean near) {
        super(cacheMetricsRegistryName(cctx.name(), near), cctx.kernalContext());

        this.cctx = cctx;

        if (cctx.store().store() instanceof GridCacheWriteBehindStore)
            store = (GridCacheWriteBehindStore<?, ?>)cctx.store().store();
        else
            store = null;
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.reads = bldr.longMetric("CacheGets", "The total number of gets to the cache.");

        hldr.entryProcessorPuts = bldr.longMetric("EntryProcessorPuts",
                "The total number of cache invocations, caused update.");

        hldr.entryProcessorRemovals = bldr.longMetric("EntryProcessorRemovals",
                "The total number of cache invocations, caused removals.");

        hldr.entryProcessorReadOnlyInvocations = bldr.longMetric("EntryProcessorReadOnlyInvocations",
                "The total number of cache invocations, caused no updates.");

        hldr.entryProcessorInvokeTimeNanos = bldr.longMetric("EntryProcessorInvokeTimeNanos",
                "The total time of cache invocations, in nanoseconds.");

        hldr.entryProcessorMinInvocationTime = bldr.longMetric("EntryProcessorMinInvocationTime",
                "So far, the minimum time to execute cache invokes.");

        hldr.entryProcessorMaxInvocationTime = bldr.longMetric("EntryProcessorMaxInvocationTime",
                "So far, the maximum time to execute cache invokes.");

        hldr.entryProcessorHits = bldr.longMetric("EntryProcessorHits",
                "The total number of invocations on keys, which exist in cache.");

        hldr.entryProcessorMisses = bldr.longMetric("EntryProcessorMisses",
                "The total number of invocations on keys, which don't exist in cache.");

        hldr.writes = bldr.longMetric("CachePuts",
                "The total number of puts to the cache.");

        hldr.hits = bldr.longMetric("CacheHits",
                "The number of get requests that were satisfied by the cache.");

        hldr.misses = bldr.longMetric("CacheMisses",
                "A miss is a get request that is not satisfied.");

        hldr.txCommits = bldr.longMetric("CacheTxCommits",
                "Total number of transaction commits.");

        hldr.txRollbacks = bldr.longMetric("CacheTxRollbacks",
                "Total number of transaction rollbacks.");

        hldr.evictCnt = bldr.longMetric("CacheEvictions",
                "The total number of evictions from the cache.");

        hldr.rmCnt = bldr.longMetric("CacheRemovals", "The total number of removals from the cache.");

        hldr.putTimeTotal = bldr.longMetric("PutTimeTotal",
                "The total time of cache puts, in nanoseconds.");

        hldr.getTimeTotal = bldr.longMetric("GetTimeTotal",
                "The total time of cache gets, in nanoseconds.");

        hldr.rmvTimeTotal = bldr.longMetric("RemovalTimeTotal",
                "The total time of cache removal, in nanoseconds.");

        hldr.commitTimeNanos = bldr.longMetric("CommitTime",
                "The total time of commit, in nanoseconds.");

        hldr.rollbackTimeNanos = bldr.longMetric("RollbackTime",
                "The total time of rollback, in nanoseconds.");

        hldr.rebalancedKeys = bldr.longMetric("RebalancedKeys",
                "Number of already rebalanced keys.");

        hldr.totalRebalancedBytes = bldr.longMetric("TotalRebalancedBytes",
                "Number of already rebalanced bytes.");

        hldr.rebalanceStartTime = bldr.longMetric("RebalanceStartTime",
                "Rebalance start time");

        hldr.rebalanceStartTime.value(-1);

        hldr.estimatedRebalancingKeys = bldr.longMetric("EstimatedRebalancingKeys",
                "Number estimated to rebalance keys.");

        hldr.rebalancingKeysRate = bldr.hitRateMetric("RebalancingKeysRate",
                "Estimated rebalancing speed in keys",
                REBALANCE_RATE_INTERVAL,
                20);

        hldr.rebalancingBytesRate = bldr.hitRateMetric("RebalancingBytesRate",
                "Estimated rebalancing speed in bytes",
                REBALANCE_RATE_INTERVAL,
                20);

        hldr.rebalanceClearingPartitions = bldr.longMetric("RebalanceClearingPartitionsLeft",
                "Number of partitions need to be cleared before actual rebalance start");

        bldr.register("OffHeapEntriesCount", this::offHeapEntriesCount,
                "Number of entries stored in off-heap memory");

        bldr.register("HeapEntriesCount", this::heapEntriesCount,
                "number of cache entries in heap memory, including entries held by active transactions, entries in\n" +
                        " onheap cache and near entries.");

        bldr.register("OffHeapPrimaryEntriesCount", this::offHeapPrimaryEntriesCount,
                "Number of primary entries stored in off-heap memory");

        bldr.register("OffHeapBackupEntriesCount", this::offHeapBackupEntriesCount,
                "Number of backup entries stored in off-heap memory");

        bldr.register("LocalSize", this::localSize, "Cache local size");

        bldr.register("WriteBehindFlushSize", this::writeBehindFlushSize,
                "Total count of entries in cache store internal buffer");

        hldr.minQryTime = bldr.longMetric("QueriesMinimalTime", null);
        hldr.minQryTime.value(Long.MAX_VALUE);

        hldr.maxQryTime = bldr.longMetric("QueriesMaximumTime", null);

        hldr.totalQryTime = bldr.longAdderMetric("QueriesTotalTime", null);

        hldr.qryExecs = bldr.longAdderMetric("QueriesExecuted", null);

        hldr.qryCompleted = bldr.longAdderMetric("QueriesCompleted", null);

        hldr.qryFails = bldr.longAdderMetric("QueriesFailed", null);

        //TODO: fix compilation
/*
        bldr.register("IsIndexRebuildInProgress", () -> {
            IgniteInternalFuture<?> fut = cctx.shared().database().indexRebuildFuture(cctx.cacheId());

            return fut != null && !fut.isDone();
        }, "True if index rebuild is in progress.");
*/

        hldr.getTime = bldr.histogram("GetTime", HISTOGRAM_BUCKETS, "Get time in nanoseconds.");

        hldr.putTime = bldr.histogram("PutTime", HISTOGRAM_BUCKETS, "Put time in nanoseconds.");

        hldr.rmvTime = bldr.histogram("RemoveTime", HISTOGRAM_BUCKETS, "Remove time in nanoseconds.");
    }

    /**
     * @param delegate Metrics to delegate to.
     */
    public void delegate(CacheMetricSource delegate) {
        this.delegate = delegate;
    }

    /**
     * Returns number of off-heap entries.
     *
     * @return Number of off-heap entries.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public long offHeapEntriesCount() {
        if (!enabled())
            return 0;

        return entriesStat().offHeapEntriesCount();
    }

    /**
     * Returns number of heap entries.
     *
     * @return Number of heap entries.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public long heapEntriesCount() {
        if (!enabled())
            return 0;

        return entriesStat().heapEntriesCount();
    }

    /**
     * Returns number of primary entries stored in off-heap memory.
     *
     * @return Number of primary entries stored in off-heap memory.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public long offHeapPrimaryEntriesCount() {
        if (!enabled())
            return 0;

        return entriesStat().offHeapPrimaryEntriesCount();
    }

    /**
     * Returns number of backup entries stored in off-heap memory.
     *
     * @return Number of backup entries stored in off-heap memory.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public long offHeapBackupEntriesCount() {
        if (!enabled())
            return 0;

        return entriesStat().offHeapBackupEntriesCount();
    }

    /**
     * Returns cache local size.
     *
     * @return Cache local size.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public long localSize() {
        if (!enabled())
            return 0;

        return entriesStat().localSize();
    }

    /**
     * Returns total number of partitions on current node.
     *
     * @return Total number of partitions on current node.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public int totalPartitionsCount() {
        if (!enabled())
            return 0;

        return entriesStat().totalPartitionsCount();
    }

    /**
     * Returns number of currently rebalancing partitions on current node.
     *
     * @return Number of currently rebalancing partitions on current node.
     */
    //TODO: Optimize: entriesStat() calculates metrics on each call.
    public int rebalancingPartitionsCount() {
        if (!enabled())
            return 0;

        return entriesStat().rebalancingPartitionsCount();
    }

    /**
     * Returns number of already rebalanced keys.
     *
     * @return Number of already rebalanced keys.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rebalancedKeys() {
        Holder hldr = holder();

        return hldr != null ? hldr.rebalancedKeys.value() : 0;
    }

    /**
     * Returns number estimated to rebalance keys.
     *
     * @return Number estimated to rebalance keys.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long estimatedRebalancingKeys() {
        Holder hldr = holder();

        return hldr != null ? hldr.estimatedRebalancingKeys.value() : 0;
    }

    /**
     * Returns estimated rebalancing speed in keys.
     *
     * @return Estimated rebalancing speed in keys.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rebalancingKeysRate() {
        Holder hldr = holder();

        return hldr != null ? hldr.rebalancingKeysRate.value() : 0;
    }

    /**
     * Returns estimated rebalancing speed in bytes.
     *
     * @return Estimated rebalancing speed in bytes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rebalancingBytesRate() {
        Holder hldr = holder();

        return hldr != null ? hldr.rebalancingBytesRate.value() : 0;
    }

    /**
     * Returns estimated rebalancing finish time.
     *
     * @return Estimated rebalancing finish time.
     */
    public long estimatedRebalancingFinishTime() {
        Holder hldr = holder();

        if (hldr == null)
            return 0;

        long rate = hldr.rebalancingKeysRate.value();

        return rate <= 0 ? -1L :
                ((keysToRebalanceLeft() / rate) * REBALANCE_RATE_INTERVAL) + U.currentTimeMillis();
    }

    /**
     * Sets clearing partitions number.
     *
     * @param partitions Partitions number.
     */
    public void rebalanceClearingPartitions(int partitions) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.rebalanceClearingPartitions.value(partitions);
    }

    /**
     * Returns number of partitions need to be cleared before actual rebalance start.
     *
     * @return Number of partitions need to be cleared before actual rebalance start.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rebalanceClearingPartitionsLeft() {
        Holder hldr = holder();

        return hldr != null ? hldr.rebalanceClearingPartitions.value() : 0;
    }

    /**
     * Returns estimated number of keys to be rebalanced on current node.
     *
     * @return Estimated number of keys to be rebalanced on current node.
     */
    //TODO: make private in Apache Ignite 3.0
    public long keysToRebalanceLeft() {
        Holder hldr = holder();

        if (hldr == null)
            return 0;

        return Math.max(0, hldr.estimatedRebalancingKeys.value() - hldr.rebalancedKeys.value());
    }

    /**
     * Returns rebalancing start time.
     *
     * @return Rebalancing start time.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rebalancingStartTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.rebalanceStartTime.value() : 0;
    }

    /**
     * Sets rebalance start time.
     */
    public void startRebalance(long delay) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.rebalanceStartTime.value(delay + U.currentTimeMillis());
    }

    /**
     * Reset rebalance counters.
     */
    public void resetRebalanceCounters() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.estimatedRebalancingKeys.reset();

            hldr.rebalancedKeys.reset();

            hldr.totalRebalancedBytes.reset();

            hldr.rebalancingBytesRate.reset();

            hldr.rebalancingKeysRate.reset();

            hldr.rebalanceStartTime.value(-1L);
        }
    }

    /**
     * First rebalance supply message callback.
     *
     * @param keysCnt Estimated number of keys.
     */
    public void onRebalancingKeysCountEstimateReceived(Long keysCnt) {
        if (keysCnt == null)
            return;

        Holder hldr = holder();

        if (hldr != null)
            hldr.estimatedRebalancingKeys.add(keysCnt);
    }

    /**
     * Rebalance entry store callback.
     */
    public void onRebalanceKeyReceived() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rebalancedKeys.increment();

            hldr.rebalancingKeysRate.increment();
        }
    }

    /**
     * Rebalance supply message callback.
     *
     * @param batchSize Batch size in bytes.
     */
    public void onRebalanceBatchReceived(long batchSize) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.totalRebalancedBytes.add(batchSize);

            hldr.rebalancingBytesRate.add(batchSize);
        }
    }

    /**
     * Returns count of entries that were processed by the write-behind store and have not been
     * flushed to the underlying store yet.
     *
     * @return Total count of entries in cache store internal buffer. {@code 0} if cache store isn't configured.
     */
    public int writeBehindFlushSize() {
        if (!enabled())
            return 0;

        return store != null ? store.getWriteBehindFlushSize() : 0;
    }

    /**
     * Returns commit transaction time taken nanos.
     *
     * @return Commit transaction time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long commitTimeNanos() {
        Holder hldr = holder();

        return hldr != null ? hldr.commitTimeNanos.value() : 0;
    }

    /**
     * Returns number of transaction commits.
     *
     * @return Number of transaction commits.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long txCommits() {
        Holder hldr = holder();

        return hldr != null ? hldr.txCommits.value() : 0;
    }

    /**
     * Returns rollback transaction time taken nanos.
     *
     * @return Rollback transaction time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long rollbackTimeNanos() {
        Holder hldr = holder();

        return hldr != null ? hldr.rollbackTimeNanos.value() : 0;
    }

    /**
     * Returns number of transaction rollbacks.
     *
     * @return Number of transaction rollbacks.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long txRollbacks() {
        Holder hldr = holder();

        return hldr != null ? hldr.txRollbacks.value() : 0;
    }

    /**
     * Returns number of cache hits.
     *
     * @return Number of cache hits.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long cacheHits() {
        Holder hldr = holder();

        return hldr != null ? hldr.hits.value() : 0;
    }

    /**
     * Returns number of reads.
     *
     * @return Number of reads.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long reads() {
        Holder hldr = holder();

        return hldr != null ? hldr.reads.value() : 0;
    }

    /**
     * Returns number of cache misses.
     *
     * @return Number of cache misses.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long misses() {
        Holder hldr = holder();

        return hldr != null ? hldr.misses.value() : 0;
    }

    /**
     * Returns number of cache writes.
     *
     * @return Number of cache writes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long writes() {
        Holder hldr = holder();

        return hldr != null ? hldr.writes.value() : 0;
    }

    /**
     * Returns number of entry processor invocations caused update.
     *
     * @return Number of entry processor invocations caused update.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorPuts() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorPuts.value() : 0;
    }

    /**
     * Returns number of entry processor invocations caused removal.
     *
     * @return Number of entry processor invocations caused removal.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorRemovals() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorRemovals.value() : 0;
    }

    /**
     * Returns number of entry processor read only invocations.
     *
     * @return Number of entry processor read only invocations.
     */
    @Deprecated
    public long entryProcessorReadOnlyInvocations() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorReadOnlyInvocations.value() : 0;
    }

    /**
     * Returns number of entry processor invokes on keys, which exist in cache.
     *
     * @return Number of entry processor invokes on keys, which exist in cache.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorHits() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorHits.value() : 0;
    }

    /**
     * Returns number of entry processor invokes on keys, which don't exist in cache.
     *
     * @return Number of entry processor invokes on keys, which don't exist in cache.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorMisses() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorMisses.value() : 0;
    }

    /**
     * Returns entry processor invoke time taken nanos.
     *
     * @return Entry processor invoke time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorInvokeTimeNanos() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorInvokeTimeNanos.value() : 0;
    }

    /**
     * Returns entry processor minimum time to execute cache invokes.
     *
     * @return Entry processor minimum time to execute cache invokes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorMinInvocationTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorMinInvocationTime.value() : 0;
    }

    /**
     * Returns entry processor maximum time to execute cache invokes.
     *
     * @return Entry processor maximum time to execute cache invokes.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long entryProcessorMaxInvocationTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.entryProcessorMaxInvocationTime.value() : 0;
    }

    /**
     * Returns number of removed cache entries.
     *
     * @return Number of removed cache entries.
     */
    @Deprecated
    public long removals() {
        Holder hldr = holder();

        return hldr != null ? hldr.rmCnt.value() : 0;
    }

    /**
     * Returns number of evictions.
     *
     * @return Number of evictions.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long evictions() {
        Holder hldr = holder();

        return hldr != null ? hldr.evictCnt.value() : 0;
    }

    /**
     * Returns cache entry get time taken nanos.
     *
     * @return Cache entry get time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long getTimeTotal() {
        Holder hldr = holder();

        return hldr != null ? hldr.getTimeTotal.value() : 0;
    }

    /**
     * Returns cache entry put time taken nanos.
     *
     * @return Cache entry put time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long putTimeTotal() {
        Holder hldr = holder();

        return hldr != null ? hldr.putTimeTotal.value() : 0;
    }

    /**
     * Returns cache entry remove time taken nanos.
     *
     * @return Cache entry remove time taken nanos.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long removeTimeTotal() {
        Holder hldr = holder();

        return hldr != null ? hldr.rmvTimeTotal.value() : null;
    }

    /**
     * Cache read callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.reads.increment();

            if (isHit)
                hldr.hits.increment();
            else
                hldr.misses.increment();

            if (delegate != null)
                delegate.onRead(isHit);
        }
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.writes.increment();

            if (delegate != null)
                delegate.onWrite();
        }
    }

    /**
     * Cache remove callback.
     */
    public void onRemove() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rmCnt.increment();

            if (delegate != null)
                delegate.onRemove();
        }
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.evictCnt.increment();

            if (delegate != null)
                delegate.onEvict();
        }
    }

    /**
     * Cache invocations caused update callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeUpdate(boolean isHit) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.entryProcessorPuts.increment();

            if (isHit)
                hldr.entryProcessorHits.increment();
            else
                hldr.entryProcessorMisses.increment();

            if (delegate != null)
                delegate.onInvokeUpdate(isHit);
        }
    }

    /**
     * Cache invocations caused removal callback.
     *
     * @param isHit Hit or miss flag.
     */
    public void onInvokeRemove(boolean isHit) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.entryProcessorRemovals.increment();

            if (isHit)
                hldr.entryProcessorHits.increment();
            else
                hldr.entryProcessorMisses.increment();

            if (delegate != null)
                delegate.onInvokeRemove(isHit);
        }
    }

    /**
     * Read-only cache invocations.
     *
     * @param isHit Hit or miss flag.
     */
    public void onReadOnlyInvoke(boolean isHit) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.entryProcessorReadOnlyInvocations.increment();

            if (isHit)
                hldr.entryProcessorHits.increment();
            else
                hldr.entryProcessorMisses.increment();

            if (delegate != null)
                delegate.onReadOnlyInvoke(isHit);
        }
    }

    /**
     * Increments invoke operation time nanos.
     *
     * @param duration Duration.
     */
    public void addInvokeTimeNanos(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.entryProcessorInvokeTimeNanos.add(duration);

            recalculateInvokeMinTimeNanos(duration, hldr);

            recalculateInvokeMaxTimeNanos(duration, hldr);

            if (delegate != null)
                delegate.addInvokeTimeNanos(duration);
        }
    }

    /**
     * Transaction commit callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxCommit(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.txCommits.increment();
            hldr.commitTimeNanos.add(duration);

            if (delegate != null)
                delegate.onTxCommit(duration);
        }
    }

    /**
     * Transaction rollback callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxRollback(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.txRollbacks.increment();
            hldr.rollbackTimeNanos.add(duration);

            if (delegate != null)
                delegate.onTxRollback(duration);
        }
    }

    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.getTimeTotal.add(duration);

            hldr.getTime.value(duration);

            if (delegate != null)
                delegate.addGetTimeNanos(duration);
        }
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.putTimeTotal.add(duration);

            hldr.putTime.value(duration);

            if (delegate != null)
                delegate.addPutTimeNanos(duration);
        }
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeTotal(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rmvTimeTotal.add(duration);

            hldr.rmvTime.value(duration);

            if (delegate != null)
                delegate.addRemoveTimeTotal(duration);
        }
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rmvTimeTotal.add(duration);
            hldr.getTimeTotal.add(duration);

            if (delegate != null)
                delegate.addRemoveAndGetTimeNanos(duration);
        }
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.putTimeTotal.add(duration);
            hldr.getTimeTotal.add(duration);

            if (delegate != null)
                delegate.addPutAndGetTimeNanos(duration);
        }
    }

    /**
     * Recalculates invoke operation minimum time nanos.
     *
     * @param duration Duration.
     */
    private void recalculateInvokeMinTimeNanos(long duration, Holder hldr) {
        long minTime = hldr.entryProcessorMinInvocationTime.value();

        while (minTime > duration || minTime == 0) {
            if (MetricUtils.compareAndSet(hldr.entryProcessorMinInvocationTime, minTime, duration))
                break;
            else
                minTime = hldr.entryProcessorMinInvocationTime.value();
        }
    }

    /**
     * Recalculates invoke operation maximum time nanos.
     *
     * @param duration Duration.
     */
    private void recalculateInvokeMaxTimeNanos(long duration, Holder hldr) {
        long maxTime = hldr.entryProcessorMaxInvocationTime.value();

        while (maxTime < duration) {
            if (MetricUtils.compareAndSet(hldr.entryProcessorMaxInvocationTime, maxTime, duration))
                break;
            else
                maxTime = hldr.entryProcessorMaxInvocationTime.value();
        }
    }

    /**
     * Returns minimum execution time of query.
     *
     * @return Minimum execution time of query.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long minQueryTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.minQryTime.value() : 0;
    }

    /**
     * Returns maximum execution time of query.
     *
     * @return Maximum execution time of query.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long maxQueryTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.maxQryTime.value() : 0;
    }

    /**
     * Returns average execution time of query.
     *
     * @return Average execution time of query.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long completedQueries() {
        Holder hldr = holder();

        return hldr != null ? hldr.qryCompleted.value() : 0;
    }

    /**
     * Returns total execution time of query.
     *
     * @return Total execution time of query.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long queriesTotalTime() {
        Holder hldr = holder();

        return hldr != null ? hldr.totalQryTime.value() : 0;
    }

    /**
     * Returns total count of executed queries.
     *
     * @return Total count of executed queries.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long queriesExecuted() {
        Holder hldr = holder();

        return hldr != null ? hldr.qryExecs.value() : 0;
    }

    /**
     * Returns total count of failed queries.
     *
     * @return Total count of failed queries.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long queriesFailed() {
        Holder hldr = holder();

        return hldr != null ? hldr.qryFails.value() : 0;
    }

    /**
     * Update metrics.
     *
     * @param duration Duration of queue execution.
     * @param fail {@code True} query executed unsuccessfully {@code false} otherwise.
     */
    // TODO: Try to avoid aggregation of values in Apache Ignte 3.0
    public void updateQueryMetrics(long duration, boolean fail) {
        Holder hldr = holder();

        if (hldr != null) {
            if (fail) {
                hldr.qryExecs.increment();
                hldr.qryFails.increment();
            }
            else {
                hldr.qryExecs.increment();
                hldr.qryCompleted.increment();

                MetricUtils.setIfLess(hldr.minQryTime, duration);
                MetricUtils.setIfGreater(hldr.maxQryTime, duration);

                hldr.totalQryTime.add(duration);
            }
        }
    }

    /** Resets query metrics. */
    public void resetQueryMetrics() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.minQryTime.value(Long.MAX_VALUE);
            hldr.maxQryTime.reset();
            hldr.totalQryTime.reset();
            hldr.qryExecs.reset();
            hldr.qryCompleted.reset();
            hldr.qryFails.reset();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param isNear Is near flag.
     * @return Cache metrics registry name.
     */
    private static String cacheMetricsRegistryName(String cacheName, boolean isNear) {
        if (isNear)
            return metricName(CACHE_METRICS, cacheName, "near");

        return metricName(CACHE_METRICS, cacheName);
    }

    /**
     * Calculates entries count/partitions count metrics using one iteration over local partitions for all metrics
     */
    //TODO: Optimize: Calculate once periodicaly not for each call.
    public EntriesStatMetrics entriesStat() {
        int owningPartCnt = 0;
        int movingPartCnt = 0;
        long offHeapEntriesCnt = 0L;
        long offHeapPrimaryEntriesCnt = 0L;
        long offHeapBackupEntriesCnt = 0L;
        long heapEntriesCnt = 0L;
        long locSize = 0L;

        try {
            final GridCacheAdapter<?, ?> cache = cctx.cache();

            if (cache != null) {
                offHeapEntriesCnt = cache.offHeapEntriesCount();

                locSize = cache.localSizeLong(null);
            }

            if (cctx.isLocal()) {
                if (cache != null) {
                    offHeapPrimaryEntriesCnt = offHeapEntriesCnt;

                    heapEntriesCnt = cache.sizeLong();
                }
            }
            else {
                AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

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
            owningPartCnt = -1;
            movingPartCnt = 0;
            offHeapEntriesCnt = -1L;
            offHeapPrimaryEntriesCnt = -1L;
            offHeapBackupEntriesCnt = -1L;
            heapEntriesCnt = -1L;
            locSize = -1L;
        }

        EntriesStatMetrics stat = new EntriesStatMetrics();

        stat.offHeapEntriesCount(offHeapEntriesCnt);
        stat.offHeapPrimaryEntriesCount(offHeapPrimaryEntriesCnt);
        stat.offHeapBackupEntriesCount(offHeapBackupEntriesCnt);
        stat.heapEntriesCount(heapEntriesCnt);
        stat.localSize(locSize);
        stat.totalPartitionsCount(owningPartCnt + movingPartCnt);
        stat.rebalancingPartitionsCount(movingPartCnt);

        return stat;
    }

    /**
     * Reset metric values.
     */
    public void reset() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.reads.reset();
            hldr.writes.reset();
            hldr.rmCnt.reset();
            hldr.hits.reset();
            hldr.misses.reset();
            hldr.evictCnt.reset();
            hldr.txCommits.reset();
            hldr.txRollbacks.reset();
            hldr.putTimeTotal.reset();
            hldr.rmvTimeTotal.reset();
            hldr.getTimeTotal.reset();
            hldr.commitTimeNanos.reset();
            hldr.rollbackTimeNanos.reset();

            hldr.entryProcessorPuts.reset();
            hldr.entryProcessorRemovals.reset();
            hldr.entryProcessorReadOnlyInvocations.reset();
            hldr.entryProcessorMisses.reset();
            hldr.entryProcessorHits.reset();
            hldr.entryProcessorInvokeTimeNanos.reset();
            hldr.entryProcessorMaxInvocationTime.reset();
            hldr.entryProcessorMinInvocationTime.reset();
            hldr.getTime.reset();
            hldr.putTime.reset();
            hldr.rmvTime.reset();

            resetRebalanceCounters();

            if (delegate != null)
                delegate.reset();
        }
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

        /** Long size. */
        private long locSize;

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
         * @return Long size.
         */
        public long localSize() {
            return locSize;
        }

        /**
         * @param size Local cache size.
         */
        public void localSize(long size) {
            locSize = size;
        }
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Number of reads. */
        private AtomicLongMetric reads;

        /** Number of entry processor invocations caused update. */
        private AtomicLongMetric entryProcessorPuts;

        /** Number of entry processor invocations caused removal. */
        private AtomicLongMetric entryProcessorRemovals;

        /** Number of entry processor read only invocations. */
        private AtomicLongMetric entryProcessorReadOnlyInvocations;

        /** Entry processor invoke time taken nanos. */
        private AtomicLongMetric entryProcessorInvokeTimeNanos;

        /** Entry processor minimum time to execute cache invokes. */
        private AtomicLongMetric entryProcessorMinInvocationTime;

        /** Entry processor maximum time to execute cache invokes. */
        private AtomicLongMetric entryProcessorMaxInvocationTime;

        /** Number of entry processor invokes on keys, which exist in cache. */
        private AtomicLongMetric entryProcessorHits;

        /** Number of entry processor invokes on keys, which don't exist in cache. */
        private AtomicLongMetric entryProcessorMisses;

        /** Number of cache writes. */
        private AtomicLongMetric writes;

        /** Number of cache hits. */
        private AtomicLongMetric hits;

        /** Number of cache misses. */
        private AtomicLongMetric misses;

        /** Number of transaction commits. */
        private AtomicLongMetric txCommits;

        /** Number of transaction rollbacks. */
        private AtomicLongMetric txRollbacks;

        /** Number of evictions. */
        private AtomicLongMetric evictCnt;

        /** Number of removed cache entries. */
        private AtomicLongMetric rmCnt;

        /** Cache entry put time taken nanos. */
        private AtomicLongMetric putTimeTotal;

        /** Cache entry get time taken nanos. */
        private AtomicLongMetric getTimeTotal;

        /** Cache entry remove time taken nanos. */
        private AtomicLongMetric rmvTimeTotal;

        /** Commit transaction time taken nanos. */
        private AtomicLongMetric commitTimeNanos;

        /** Rollback transaction time taken nanos. */
        private AtomicLongMetric rollbackTimeNanos;

        /** Rebalanced keys count. */
        private AtomicLongMetric rebalancedKeys;

        /** Total rebalanced bytes count. */
        private AtomicLongMetric totalRebalancedBytes;

        /** Rebalanced start time. */
        private AtomicLongMetric rebalanceStartTime;

        /** Estimated rebalancing keys count. */
        private AtomicLongMetric estimatedRebalancingKeys;

        /** Rebalancing rate in keys. */
        private HitRateMetric rebalancingKeysRate;

        /** Rebalancing rate in bytes. */
        private HitRateMetric rebalancingBytesRate;

        /** Number of currently clearing partitions for rebalancing. */
        private AtomicLongMetric rebalanceClearingPartitions;

        /** Minimum time of queries execution. */
        private AtomicLongMetric minQryTime;

        /** Maximum time of queries execution. */
        private AtomicLongMetric maxQryTime;

        /** Sum of queries execution time for all completed queries. */
        private LongAdderMetric totalQryTime;

        /** Number of queries executions. */
        private LongAdderMetric qryExecs;

        /** Number of completed queries executions. */
        private LongAdderMetric qryCompleted;

        /** Number of failed queries. */
        private LongAdderMetric qryFails;

        /** Get time. */
        private HistogramMetricImpl getTime;

        /** Put time. */
        private HistogramMetricImpl putTime;

        /** Remove time. */
        private HistogramMetricImpl rmvTime;
    }
}
