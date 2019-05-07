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
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheGroupMetricsMXBeanImpl.GroupAllocationTracker;
import org.apache.ignite.internal.processors.monitoring.GridMonitoringManager;
import org.apache.ignite.internal.processors.monitoring.MonitoringGroup;
import org.apache.ignite.internal.processors.monitoring.sensor.DoubleConcurrentSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.HitRateSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.LongAdderSensor;
import org.apache.ignite.internal.processors.monitoring.sensor.SensorGroup;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * @deprecated Use {@link GridMonitoringManager} instead.
 */
@Deprecated
public class DataRegionMetricsImpl implements DataRegionMetrics, AllocatedPageTracker {
    /** */
    private final DataRegionMetricsProvider dataRegionMetricsProvider;

    /** */
    private final LongAdderSensor totalAllocatedPages;

    /** */
    private final ConcurrentMap<Integer, GroupAllocationTracker> grpAllocationTrackers = new ConcurrentHashMap<>();

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final DoubleConcurrentSensor largeEntriesPages;

    /** Counter for number of dirty pages. */
    private final LongAdderSensor dirtyPages;

    /** */
    private final LongAdderSensor readPages;

    /** */
    private final LongAdderSensor writtenPages;

    /** */
    private final LongAdderSensor replacedPages;

    /** */
    private final LongAdderSensor offHeapSize;

    /** */
    private final LongAdderSensor checkpointBufferSize;

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private boolean persistenceEnabled;

    /** */
    private volatile int subInts;

    /** Allocation rate calculator. */
    private volatile HitRateSensor allocRate;

    /** Eviction rate calculator. */
    private volatile HitRateSensor evictRate;

    /** */
    private volatile HitRateSensor pageReplaceRate;

    /** */
    private volatile HitRateSensor pageReplaceAge;

    /** */
    private final DataRegionConfiguration memPlcCfg;

    /** */
    private PageMemory pageMem;

    /** Time interval (in milliseconds) when allocations/evictions are counted to calculate rate. */
    private volatile long rateTimeInterval;

    /**
     * @param memPlcCfg DataRegionConfiguration.
     */
    public DataRegionMetricsImpl(GridMonitoringManager mgr, DataRegionConfiguration memPlcCfg) {
        this(mgr, memPlcCfg, null);
    }

    /**
     * @param memPlcCfg DataRegionConfiguration.
     */
    public DataRegionMetricsImpl(GridMonitoringManager mgr, DataRegionConfiguration memPlcCfg,
                                 @Nullable DataRegionMetricsProvider dataRegionMetricsProvider) {
        this.memPlcCfg = memPlcCfg;
        this.dataRegionMetricsProvider = dataRegionMetricsProvider;

        metricsEnabled = memPlcCfg.isMetricsEnabled();

        rateTimeInterval = memPlcCfg.getMetricsRateTimeInterval();

        subInts = memPlcCfg.getMetricsSubIntervalCount();

        SensorGroup drSensors = mgr.sensorsGroup(MonitoringGroup.DATA_REGIONS, memPlcCfg.getName());

        totalAllocatedPages = drSensors.longAdderSensor("TotalAllocatedPages");

        largeEntriesPages = drSensors.doubleConcurrentSensor("LargeEntriesPagesPercentage", v -> {
            return totalAllocatedPages.getValue() != 0 ?
                v / totalAllocatedPages.getValue()
                : 0;
        });

        dirtyPages = drSensors.longAdderSensor("DirtyPages");

        readPages = drSensors.longAdderSensor("PagesRead");

        writtenPages = drSensors.longAdderSensor("PagesWritten");

        replacedPages = drSensors.longAdderSensor("PagesReplaced");

        offHeapSize = drSensors.longAdderSensor("OffHeapSize");

        checkpointBufferSize = drSensors.longAdderSensor("CheckpointBufferSize");

        allocRate = drSensors.hitRateSensor("AllocationRate", 60_000, 5);

        evictRate = drSensors.hitRateSensor("EvictionRate", 60_000, 5);

        pageReplaceRate = drSensors.hitRateSensor("PagesReplaceRate", 60_000, 5);

        pageReplaceAge = drSensors.hitRateSensor("PagesReplaceAge", 60_000, 5);

        drSensors.sensor("MaxSize", (int) (memPlcCfg.getMaxSize() / (1024 * 1024)));
        drSensors.sensor("SwapPath", memPlcCfg.getSwapPath() == null ? "" : memPlcCfg.getSwapPath());
        drSensors.sensor("InitialSize", (int) (memPlcCfg.getInitialSize() / (1024 * 1024)));
        drSensors.sensor("PagesFillFactor", this::getPagesFillFactor);
        drSensors.sensor("PhysicalMemoryPages", this::getPhysicalMemoryPages);
        drSensors.sensor("TotalUsedPages", this::getTotalUsedPages);
        drSensors.sensor("OffheapUsedSize", this::getOffheapUsedSize);
        drSensors.sensor("Name", memPlcCfg.getName());
        drSensors.sensor("PageSize", () -> pageMem.pageSize());
        drSensors.sensor("TotalAllocatedSize", this::getTotalAllocatedSize);
        drSensors.sensor("PhysicalMemorySize", this::getPhysicalMemorySize);
        drSensors.sensor("UsedCheckpointBufferPages", this::getUsedCheckpointBufferPages);
        drSensors.sensor("UsedCheckpointBufferSize", this::getUsedCheckpointBufferSize);

    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return U.maskName(memPlcCfg.getName());
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return totalAllocatedPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return getTotalAllocatedPages() - dataRegionMetricsProvider.emptyDataPages();
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

        return ((float)allocRate.getValue() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)evictRate.getValue() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (!metricsEnabled)
            return 0;

        return totalAllocatedPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        if (!metricsEnabled || dataRegionMetricsProvider == null)
            return 0;

        long freeSpace = dataRegionMetricsProvider.partiallyFilledPagesFreeSpace();

        long totalAllocated = getPageSize() * totalAllocatedPages.getValue();

        return totalAllocated != 0 ?
            (float) (totalAllocated - freeSpace) / totalAllocated
            : 0f;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return dirtyPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return ((float)pageReplaceRate.getValue() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        long rep = pageReplaceRate.getValue();

        return rep == 0 ? 0 : ((float)pageReplaceAge.getValue() / rep);
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

        return checkpointBufferSize.getValue();
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

        return readPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        if (!metricsEnabled)
            return 0;

        return writtenPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        if (!metricsEnabled)
            return 0;

        return replacedPages.getValue();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return offHeapSize.getValue();
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
        this.offHeapSize.add(size);
    }

    /**
     * @param size Checkpoint buffer size.
     */
    public void updateCheckpointBufferSize(long size) {
        this.checkpointBufferSize.add(size);
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

        allocRate = new HitRateSensor((int) rateTimeInterval, subInts);
        evictRate = new HitRateSensor((int) rateTimeInterval, subInts);
        pageReplaceRate = new HitRateSensor((int)rateTimeInterval, subInts);
        pageReplaceAge = new HitRateSensor((int)rateTimeInterval, subInts);
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

        allocRate = new HitRateSensor((int) rateTimeInterval, subInts);
        evictRate = new HitRateSensor((int) rateTimeInterval, subInts);
        pageReplaceRate = new HitRateSensor((int)rateTimeInterval, subInts);
        pageReplaceAge = new HitRateSensor((int)rateTimeInterval, subInts);
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        totalAllocatedPages.reset();
        grpAllocationTrackers.values().forEach(GroupAllocationTracker::reset);
        largeEntriesPages.reset();
        dirtyPages.reset();
        readPages.reset();
        writtenPages.reset();
        replacedPages.reset();
        offHeapSize.reset();
        checkpointBufferSize.reset();
        allocRate.reset();
        evictRate.reset();
        pageReplaceRate.reset();
        pageReplaceAge.reset();
    }
}
