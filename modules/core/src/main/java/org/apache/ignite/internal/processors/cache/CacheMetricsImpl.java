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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.store.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.concurrent.atomic.*;

/**
 * Adapter for cache metrics.
 */
public class CacheMetricsImpl implements CacheMetrics {
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
    private AtomicLong removeTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong commitTimeNanos = new AtomicLong();

    /** Commit transaction time taken nanos. */
    private AtomicLong rollbackTimeNanos = new AtomicLong();

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
    @Override public long getOverflowSize() {
        try {
            return cctx.cache().overflowSize();
        }
        catch (IgniteCheckedException ignored) {
            return -1;
        }
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapEntriesCount() {
        return cctx.cache().offHeapEntriesCount();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapAllocatedSize() {
        return cctx.cache().offHeapAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public int getSize() {
        return cctx.cache().size();
    }

    /** {@inheritDoc} */
    @Override public int getKeySize() {
        return cctx.cache().size();
    }

    /** {@inheritDoc} */
    @Override public boolean isEmpty() {
        return cctx.cache().isEmpty();
    }

    /** {@inheritDoc} */
    @Override public int getDhtEvictQueueCurrentSize() {
        return cctx.isNear() ?
                dhtCtx != null ? dhtCtx.evicts().evictQueueSize() : -1
                : cctx.evicts().evictQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxCommitQueueSize() {
        return cctx.tm().commitQueueSize();
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
        return cctx.tm().prepareQueueSize();
    }

    /** {@inheritDoc} */
    @Override public int getTxStartVersionCountsSize() {
        return cctx.tm().startVersionCountsSize();
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
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().threadMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtXidMapSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().idMapSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtCommitQueueSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().commitQueueSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtPrepareQueueSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().prepareQueueSize() : -1;
    }

    /** {@inheritDoc} */
    @Override public int getTxDhtStartVersionCountsSize() {
        return cctx.isNear() && dhtCtx != null ? dhtCtx.tm().startVersionCountsSize() : -1;
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
        removeTimeNanos.set(0);
        getTimeNanos.set(0);
        commitTimeNanos.set(0);
        rollbackTimeNanos.set(0);

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
        long timeNanos = removeTimeNanos.get();
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
        removeTimeNanos.addAndGet(duration);

        if (delegate != null)
            delegate.addRemoveTimeNanos(duration);
    }

    /**
     * Increments remove and get time accumulators.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveAndGetTimeNanos(long duration) {
        removeTimeNanos.addAndGet(duration);
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
        return cctx.config().getKeyType().getName();
    }

    /** {@inheritDoc} */
    @Override public String getValueType() {
        return cctx.config().getValueType().getName();
    }

    /** {@inheritDoc} */
    @Override public boolean isReadThrough() {
        return cctx.config().isReadThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isWriteThrough() {
        return cctx.config().isWriteThrough();
    }

    /** {@inheritDoc} */
    @Override public boolean isStoreByValue() {
        return cctx.config().isStoreByValue();
    }

    /** {@inheritDoc} */
    @Override public boolean isStatisticsEnabled() {
        return cctx.config().isStatisticsEnabled();
    }

    /** {@inheritDoc} */
    @Override public boolean isManagementEnabled() {
        return cctx.config().isManagementEnabled();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheMetricsImpl.class, this);
    }
}
