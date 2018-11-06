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

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.ratemetrics.HitRateMetrics;
import org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

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

    /** Number of reads. */
    private AtomicLong reads = new AtomicLong();

    /** Number of writes. */
    private AtomicLong writes = new AtomicLong();

    /** Number of hits. */
    private AtomicLong hits = new AtomicLong();

    /** Number of misses. */
    private AtomicLong misses = new AtomicLong();

    /** Number of transaction commits. */
    private AtomicLong txCommits = new AtomicLong();

    /** Number of transaction rollbacks. */
    private AtomicLong txRollbacks = new AtomicLong();

    /** Number of evictions. */
    private AtomicLong evictCnt = new AtomicLong();

    /** Number of removed entries. */
    private AtomicLong rmCnt = new AtomicLong();

    /** Put time taken nanos. */
    private AtomicLong putTimeNanos = new AtomicLong();

    /** Get time taken nanos. */
    private AtomicLong getTimeNanos = new AtomicLong();

    /** Remove time taken nanos. */
    private AtomicLong rmvTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong commitTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong rollbackTimeNanos = new AtomicLong();

    /** Number of reads from off-heap memory. */
    private AtomicLong offHeapGets = new AtomicLong();

    /** Number of writes to off-heap memory. */
    private AtomicLong offHeapPuts = new AtomicLong();

    /** Number of removed entries from off-heap memory. */
    private AtomicLong offHeapRemoves = new AtomicLong();

    /** Number of evictions from off-heap memory. */
    private AtomicLong offHeapEvicts = new AtomicLong();

    /** Number of off-heap hits. */
    private AtomicLong offHeapHits = new AtomicLong();

    /** Number of off-heap misses. */
    private AtomicLong offHeapMisses = new AtomicLong();

    /** Rebalanced keys count. */
    private AtomicLong rebalancedKeys = new AtomicLong();

    /** Total rebalanced bytes count. */
    private AtomicLong totalRebalancedBytes = new AtomicLong();

    /** Rebalanced start time. */
    private AtomicLong rebalanceStartTime = new AtomicLong(-1L);

    /** Estimated rebalancing keys count. */
    private AtomicLong estimatedRebalancingKeys = new AtomicLong();

    /** Rebalancing rate in keys. */
    private HitRateMetrics rebalancingKeysRate = new HitRateMetrics(REBALANCE_RATE_INTERVAL, 20);

    /** Rebalancing rate in bytes. */
    private HitRateMetrics rebalancingBytesRate = new HitRateMetrics(REBALANCE_RATE_INTERVAL, 20);

    /** Number of currently clearing partitions for rebalancing. */
    private AtomicLong rebalanceClearingPartitions = new AtomicLong();

    /** Cache metrics. */
    @GridToStringExclude
    private transient CacheMetricsImpl delegate;

    /** Cache context. */
    private GridCacheContext<?, ?> cctx;

    /** DHT context. */
    private GridCacheContext<?, ?> dhtCtx;

    /** Write-behind store, if configured. */
    private GridCacheWriteBehindStore store;

    /**
     * Creates cache metrics;
     *
     * @param cctx Cache context.
     */
    public CacheMetricsImpl(GridCacheContext<?, ?> cctx) {
        assert cctx != null;

        this.cctx = cctx;

        if (cctx.isNear())
            dhtCtx = cctx.near().dht().context();

        if (cctx.store().store() instanceof GridCacheWriteBehindStore)
            store = (GridCacheWriteBehindStore)cctx.store().store();

        delegate = null;
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
        return offHeapGets.get();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPuts() {
        return offHeapPuts.get();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapRemovals() {
        return offHeapRemoves.get();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEvictions() {
        return offHeapEvicts.get();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapHits() {
        return offHeapHits.get();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapHitPercentage() {
        long hits0 = offHeapHits.get();
        long gets0 = offHeapGets.get();

        if (hits0 == 0)
            return 0;

        return (float) hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapMisses() {
        return offHeapMisses.get();
    }

    /** {@inheritDoc} */
    @Override public float getOffHeapMissPercentage() {
        long misses0 = offHeapMisses.get();
        long reads0 = offHeapGets.get();

        if (misses0 == 0)
            return 0;

        return (float) misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return getEntriesStat().offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getHeapEntriesCount() {
        return getEntriesStat().heapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapPrimaryEntriesCount() {
        return getEntriesStat().offHeapPrimaryEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapBackupEntriesCount() {
        return getEntriesStat().offHeapBackupEntriesCount();
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
        return getEntriesStat().cacheSize();
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
        long timeNanos = commitTimeNanos.get();
        long commitsCnt = txCommits.get();

        if (timeNanos == 0 || commitsCnt == 0)
            return 0;

        return ((1f * timeNanos) / commitsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageTxRollbackTime() {
        long timeNanos = rollbackTimeNanos.get();
        long rollbacksCnt = txRollbacks.get();

        if (timeNanos == 0 || rollbacksCnt == 0)
            return 0;

        return ((1f * timeNanos) / rollbacksCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxCommits() {
        return txCommits.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheTxRollbacks() {
        return txRollbacks.get();
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        reads.set(0);
        writes.set(0);
        rmCnt.set(0);
        hits.set(0);
        misses.set(0);
        evictCnt.set(0);
        txCommits.set(0);
        txRollbacks.set(0);
        putTimeNanos.set(0);
        rmvTimeNanos.set(0);
        getTimeNanos.set(0);
        commitTimeNanos.set(0);
        rollbackTimeNanos.set(0);

        offHeapGets.set(0);
        offHeapPuts.set(0);
        offHeapRemoves.set(0);
        offHeapHits.set(0);
        offHeapMisses.set(0);
        offHeapEvicts.set(0);

        clearRebalanceCounters();

        if (delegate != null)
            delegate.clear();
    }

    /** {@inheritDoc} */
    @Override public long getCacheHits() {
        return hits.get();
    }

    /** {@inheritDoc} */
    @Override public float getCacheHitPercentage() {
        long hits0 = hits.get();
        long gets0 = reads.get();

        if (hits0 == 0)
            return 0;

        return (float) hits0 / gets0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheMisses() {
        return misses.get();
    }

    /** {@inheritDoc} */
    @Override public float getCacheMissPercentage() {
        long misses0 = misses.get();
        long reads0 = reads.get();

        if (misses0 == 0)
            return 0;

        return (float) misses0 / reads0 * 100.0f;
    }

    /** {@inheritDoc} */
    @Override public long getCacheGets() {
        return reads.get();
    }

    /** {@inheritDoc} */
    @Override public long getCachePuts() {
        return writes.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheRemovals() {
        return rmCnt.get();
    }

    /** {@inheritDoc} */
    @Override public long getCacheEvictions() {
        return evictCnt.get();
    }

    /** {@inheritDoc} */
    @Override public float getAverageGetTime() {
        long timeNanos = getTimeNanos.get();
        long readsCnt = reads.get();

        if (timeNanos == 0 || readsCnt == 0)
            return 0;

        return ((1f * timeNanos) / readsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAveragePutTime() {
        long timeNanos = putTimeNanos.get();
        long putsCnt = writes.get();

        if (timeNanos == 0 || putsCnt == 0)
            return 0;

        return ((1f * timeNanos) / putsCnt) / NANOS_IN_MICROSECOND;
    }

    /** {@inheritDoc} */
    @Override public float getAverageRemoveTime() {
        long timeNanos = rmvTimeNanos.get();
        long removesCnt = rmCnt.get();

        if (timeNanos == 0 || removesCnt == 0)
            return 0;

        return ((1f * timeNanos) / removesCnt) / NANOS_IN_MICROSECOND;
    }

    /**
     * Cache read callback.
     * @param isHit Hit or miss flag.
     */
    public void onRead(boolean isHit) {
        reads.incrementAndGet();

        if (isHit)
            hits.incrementAndGet();
        else
            misses.incrementAndGet();

        if (delegate != null)
            delegate.onRead(isHit);
    }

    /**
     * Cache write callback.
     */
    public void onWrite() {
        writes.incrementAndGet();

        if (delegate != null)
            delegate.onWrite();
    }

    /**
     * Cache remove callback.
     */
    public void onRemove(){
        rmCnt.incrementAndGet();

        if (delegate != null)
            delegate.onRemove();
    }

    /**
     * Cache remove callback.
     */
    public void onEvict() {
        evictCnt.incrementAndGet();

        if (delegate != null)
            delegate.onEvict();
    }

    /**
     * Transaction commit callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxCommit(long duration) {
        txCommits.incrementAndGet();
        commitTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.onTxCommit(duration);
    }

    /**
     * Transaction rollback callback.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void onTxRollback(long duration) {
        txRollbacks.incrementAndGet();
        rollbackTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.onTxRollback(duration);
    }

    /**
     * Increments the get time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        getTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addGetTimeNanos(duration);
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        putTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addPutTimeNanos(duration);
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        rmvTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addRemoveTimeNanos(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        rmvTimeNanos.addAndGet(duration);
        getTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addRemoveAndGetTimeNanos(duration);
    }

    /**
     * Increments put and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutAndGetTimeNanos(long duration) {
        putTimeNanos.addAndGet(duration);
        getTimeNanos.addAndGet(duration);

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
            if (cctx.isLocal()) {
                if (cctx.cache() != null) {
                    offHeapEntriesCnt = cctx.cache().offHeapEntriesCount();

                    offHeapPrimaryEntriesCnt = offHeapEntriesCnt;
                    offHeapBackupEntriesCnt = offHeapEntriesCnt;

                    size = cctx.cache().size();
                    sizeLong = cctx.cache().sizeLong();

                    heapEntriesCnt = sizeLong;
                }
            }
            else {
                AffinityTopologyVersion topVer = cctx.affinity().affinityTopologyVersion();

                Set<Integer> primaries = cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer);
                Set<Integer> backups = cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

                if (cctx.isNear() && cctx.cache() != null)
                    heapEntriesCnt = cctx.cache().nearSize();

                for (GridDhtLocalPartition part : cctx.topology().currentLocalPartitions()) {
                    // Partitions count.
                    GridDhtPartitionState partState = part.state();

                    if (partState == GridDhtPartitionState.OWNING)
                        owningPartCnt++;

                    if (partState == GridDhtPartitionState.MOVING)
                        movingPartCnt++;

                    // Offheap entries count
                    if (cctx.cache() == null)
                        continue;

                    long cacheSize = part.dataStore().cacheSize(cctx.cacheId());

                    offHeapEntriesCnt += cacheSize;

                    if (primaries.contains(part.id()))
                        offHeapPrimaryEntriesCnt += cacheSize;

                    if (backups.contains(part.id()))
                        offHeapBackupEntriesCnt += cacheSize;

                    size = (int)offHeapEntriesCnt;

                    heapEntriesCnt += part.publicSize(cctx.cacheId());
                }

                sizeLong = offHeapEntriesCnt;
            }
        }
        catch (Exception e) {
            owningPartCnt = -1;
            movingPartCnt = 0;
            offHeapEntriesCnt = -1L;
            offHeapPrimaryEntriesCnt = -1L;
            offHeapBackupEntriesCnt = -1L;
            heapEntriesCnt = -1L;
            size = -1;
            sizeLong = -1L;
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
        return rebalancedKeys.get();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingKeys() {
        return estimatedRebalancingKeys.get();
    }

    /** {@inheritDoc} */
    @Override public long getKeysToRebalanceLeft() {
        return Math.max(0, estimatedRebalancingKeys.get() - rebalancedKeys.get());
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingKeysRate() {
        return rebalancingKeysRate.getRate();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingBytesRate() {
        return rebalancingBytesRate.getRate();
    }

    /**
     * Clear rebalance counters.
     */
    public void clearRebalanceCounters() {
        estimatedRebalancingKeys.set(0);

        rebalancedKeys.set(0);

        totalRebalancedBytes.set(0);

        rebalancingBytesRate.clear();

        rebalancingKeysRate.clear();

        rebalanceStartTime.set(-1L);
    }

    /**
     *
     */
    public void startRebalance(long delay){
        rebalanceStartTime.set(delay + U.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public long estimateRebalancingFinishTime() {
        return getEstimatedRebalancingFinishTime();
    }

    /** {@inheritDoc} */
    @Override public long rebalancingStartTime() {
        return rebalanceStartTime.get();
    }

    /** {@inheritDoc} */
    @Override public long getEstimatedRebalancingFinishTime() {
        long rate = rebalancingKeysRate.getRate();

        return rate <= 0 ? -1L :
            ((getKeysToRebalanceLeft() / rate) * REBALANCE_RATE_INTERVAL) + U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public long getRebalancingStartTime() {
        return rebalanceStartTime.get();
    }

    /** {@inheritDoc} */
    @Override public long getRebalanceClearingPartitionsLeft() {
        return rebalanceClearingPartitions.get();
    }

    /**
     * Sets clearing partitions number.
     * @param partitions Partitions number.
     */
    public void rebalanceClearingPartitions(int partitions) {
        rebalanceClearingPartitions.set(partitions);
    }

    /**
     * First rebalance supply message callback.
     * @param keysCnt Estimated number of keys.
     */
    public void onRebalancingKeysCountEstimateReceived(Long keysCnt) {
        if (keysCnt == null)
            return;

        estimatedRebalancingKeys.addAndGet(keysCnt);
    }

    /**
     * Rebalance entry store callback.
     */
    public void onRebalanceKeyReceived() {
        rebalancedKeys.incrementAndGet();

        rebalancingKeysRate.onHit();
    }

    /**
     * Rebalance supply message callback.
     *
     * @param batchSize Batch size in bytes.
     */
    public void onRebalanceBatchReceived(long batchSize) {
        totalRebalancedBytes.addAndGet(batchSize);

        rebalancingBytesRate.onHits(batchSize);
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
        offHeapGets.incrementAndGet();

        if (hit)
            offHeapHits.incrementAndGet();
        else
            offHeapMisses.incrementAndGet();

        if (delegate != null)
            delegate.onOffHeapRead(hit);
    }

    /**
     * Off-heap write callback.
     */
    public void onOffHeapWrite() {
        offHeapPuts.incrementAndGet();

        if (delegate != null)
            delegate.onOffHeapWrite();
    }

    /**
     * Off-heap remove callback.
     */
    public void onOffHeapRemove() {
        offHeapRemoves.incrementAndGet();

        if (delegate != null)
            delegate.onOffHeapRemove();
    }

    /**
     * Off-heap evict callback.
     */
    public void onOffHeapEvict() {
        offHeapEvicts.incrementAndGet();

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
