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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMXBeanImpl.GroupAllocationTracker;
import org.apache.ignite.internal.processors.cache.ratemetrics.HitRateMetrics;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DataRegionMetricsImpl implements DataRegionMetrics, AllocatedPageTracker {
    /** */
    private final IgniteOutClosure<Long> freeSpaceProvider;

    /** */
    private final LongAdder totalAllocatedPages = new LongAdder();

    /** */
    private final ConcurrentMap<Integer, GroupAllocationTracker> grpAllocationTrackers = new ConcurrentHashMap<>();

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final LongAdder largeEntriesPages = new LongAdder();

    /** Counter for number of dirty pages. */
    private final LongAdder dirtyPages = new LongAdder();

    /** */
    private final LongAdder readPages = new LongAdder();

    /** */
    private final LongAdder writtenPages = new LongAdder();

    /** */
    private final LongAdder replacedPages = new LongAdder();

    /** */
    private final AtomicLong offHeapSize = new AtomicLong();

    /** */
    private final AtomicLong checkpointBufferSize = new AtomicLong();

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private boolean persistenceEnabled;

    /** */
    private volatile int subInts;

    /** Allocation rate calculator. */
    private volatile HitRateMetrics allocRate = new HitRateMetrics(60_000, 5);

    /** Eviction rate calculator. */
    private volatile HitRateMetrics evictRate = new HitRateMetrics(60_000, 5);

    /** */
    private volatile HitRateMetrics pageReplaceRate = new HitRateMetrics(60_000, 5);

    /** */
    private volatile HitRateMetrics pageReplaceAge = new HitRateMetrics(60_000, 5);

    /** */
    private final DataRegionConfiguration memPlcCfg;

    /** */
    private PageMemory pageMem;

    /** Time interval (in milliseconds) when allocations/evictions are counted to calculate rate. */
    private volatile long rateTimeInterval;

    /**
     * @param memPlcCfg DataRegionConfiguration.
     */
    public DataRegionMetricsImpl(DataRegionConfiguration memPlcCfg) {
        this(memPlcCfg, null);
    }

    /**
     * @param memPlcCfg DataRegionConfiguration.
     */
    public DataRegionMetricsImpl(DataRegionConfiguration memPlcCfg, @Nullable IgniteOutClosure<Long> freeSpaceProvider) {
        this.memPlcCfg = memPlcCfg;
        this.freeSpaceProvider = freeSpaceProvider;

        metricsEnabled = memPlcCfg.isMetricsEnabled();

        rateTimeInterval = memPlcCfg.getMetricsRateTimeInterval();

        subInts = memPlcCfg.getMetricsSubIntervalCount();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return U.maskName(memPlcCfg.getName());
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        if (!metricsEnabled)
            return 0;

        return totalAllocatedPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        assert pageMem != null;

        return getTotalAllocatedPages() * (persistenceEnabled ? pageMem.pageSize() : pageMem.systemPageSize());
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)allocRate.getRate() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)evictRate.getRate() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (!metricsEnabled)
            return 0;

        return totalAllocatedPages.longValue() != 0 ?
                (float) largeEntriesPages.doubleValue() / totalAllocatedPages.longValue()
                : 0;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        if (!metricsEnabled || freeSpaceProvider == null)
            return 0;

        long freeSpace = freeSpaceProvider.apply();

        long totalAllocated = getPageSize() * totalAllocatedPages.longValue();

        return (float) (totalAllocated - freeSpace) / totalAllocated;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return dirtyPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return ((float)pageReplaceRate.getRate() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        long rep = pageReplaceRate.getRate();

        return rep == 0 ? 0 : ((float)pageReplaceAge.getRate() / rep);
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        if (!persistenceEnabled)
            return getTotalAllocatedPages();

        if (!metricsEnabled)
            return 0;

        assert pageMem != null;

        return pageMem.loadedPages();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemorySize() {
        return getPhysicalMemoryPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        assert pageMem != null;

        return pageMem.checkpointBufferPagesCount();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return getUsedCheckpointBufferPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return checkpointBufferSize.get();
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        if (!metricsEnabled)
            return 0;

        assert pageMem != null;

        return pageMem.pageSize();
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        if (!metricsEnabled)
            return 0;

        return readPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        if (!metricsEnabled)
            return 0;

        return writtenPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        if (!metricsEnabled)
            return 0;

        return replacedPages.longValue();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        if (!metricsEnabled)
            return 0;

        return offHeapSize.get();
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        if (!metricsEnabled)
            return 0;

        return pageMem.loadedPages() * pageMem.systemPageSize();
    }

    /**
     * @param size Region size.
     */
    public void updateOffHeapSize(long size) {
        this.offHeapSize.addAndGet(size);
    }

    /**
     * @param size Checkpoint buffer size.
     */
    public void updateCheckpointBufferSize(long size) {
        this.checkpointBufferSize.addAndGet(size);
    }

    /**
     * Updates pageReplaceRate metric.
     */
    public void updatePageReplaceRate(long pageAge) {
        if (metricsEnabled) {
            pageReplaceRate.onHit();

            pageReplaceAge.onHits(pageAge);

            replacedPages.increment();
        }
    }

    /**
     * Updates page read.
     */
    public void onPageRead(){
        if (metricsEnabled)
            readPages.increment();
    }

    /**
     * Updates page written.
     */
    public void onPageWritten(){
        if (metricsEnabled)
            writtenPages.increment();
    }

    /**
     * Increments dirtyPages counter.
     */
    public void incrementDirtyPages() {
        if (metricsEnabled)
            dirtyPages.increment();
    }

    /**
     * Decrements dirtyPages counter.
     */
    public void decrementDirtyPages() {
        if (metricsEnabled)
            dirtyPages.decrement();
    }

    /**
     * Resets dirtyPages counter to zero.
     */
    public void resetDirtyPages() {
        if (metricsEnabled)
            dirtyPages.reset();
    }

    /** {@inheritDoc} */
    @Override public void updateTotalAllocatedPages(long delta) {
        totalAllocatedPages.add(delta);

        if (metricsEnabled && delta > 0)
            updateAllocationRateMetrics(delta);
    }

    /**
     * Get or allocate group allocation tracker.
     *
     * @param grpId Group id.
     * @return Group allocation tracker.
     */
    public GroupAllocationTracker getOrAllocateGroupPageAllocationTracker(int grpId) {
        GroupAllocationTracker tracker = grpAllocationTrackers.get(grpId);

        if (tracker == null) {
            tracker = new GroupAllocationTracker(this);

            GroupAllocationTracker old = grpAllocationTrackers.putIfAbsent(grpId, tracker);

            if (old != null)
                return old;
        }

        return tracker;
    }

    /**
     *
     */
    private void updateAllocationRateMetrics(long hits) {
        allocRate.onHits(hits);
    }

    /**
     * Updates eviction rate metric.
     */
    public void updateEvictionRate() {
        if (metricsEnabled)
            evictRate.onHit();
    }

    /**
     * @param intervalNum Interval number.
     */
    private long subInt(int intervalNum) {
        return (rateTimeInterval * intervalNum) / subInts;
    }

    /**
     *
     */
    public void incrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.increment();
    }

    /**
     *
     */
    public void decrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.decrement();
    }

    /**
     * Enable metrics.
     */
    public void enableMetrics() {
        metricsEnabled = true;
    }

    /**
     * Disable metrics.
     */
    public void disableMetrics() {
        metricsEnabled = false;
    }

    /**
     * @param persistenceEnabled Persistence enabled.
     */
    public void persistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
    }

    /**
     * @param pageMem Page mem.
     */
    public void pageMemory(PageMemory pageMem) {
        this.pageMem = pageMem;
    }

    /**
     * @param rateTimeInterval Time interval (in milliseconds) used to calculate allocation/eviction rate.
     */
    public void rateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        allocRate = new HitRateMetrics((int) rateTimeInterval, subInts);
        evictRate = new HitRateMetrics((int) rateTimeInterval, subInts);
        pageReplaceRate = new HitRateMetrics((int)rateTimeInterval, subInts);
        pageReplaceAge = new HitRateMetrics((int)rateTimeInterval, subInts);
    }

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate.
     *
     * @param subInts Number of subintervals.
     */
    public void subIntervals(int subInts) {
        assert subInts > 0;

        if (this.subInts == subInts)
            return;

        if (rateTimeInterval / subInts < 10)
            subInts = (int) rateTimeInterval / 10;

        allocRate = new HitRateMetrics((int) rateTimeInterval, subInts);
        evictRate = new HitRateMetrics((int) rateTimeInterval, subInts);
        pageReplaceRate = new HitRateMetrics((int)rateTimeInterval, subInts);
        pageReplaceAge = new HitRateMetrics((int)rateTimeInterval, subInts);
    }
}
