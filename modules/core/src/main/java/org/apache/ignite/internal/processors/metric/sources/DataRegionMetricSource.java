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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.apache.ignite.spi.metric.Metric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Datat region metric source.
 */
public class DataRegionMetricSource extends AbstractMetricSource<DataRegionMetricSource.Holder> {
    /**
     * Data region metrics prefix.
     * Full name will contain {@link DataRegionConfiguration#getName()} also.
     * {@code "io.dataregion.default"}, for example.
     */
    public static final String DATAREGION_METRICS_PREFIX = metricName("io", "dataregion");

    /** No-po implementation for page tracker. */
    private static final LongAdderMetric NO_OP =
            new LongAdderMetric("TotalAllocatedPages", "Total number of allocated pages.") {
                @Override public void add(long x) {
                    // No-op.
                }

                @Override public void reset() {
                    // No-op.
                }
            };

    /**
     * Counter for total allocated pages. Mustn't be in holder because there is no way to calculate this value after
     * disabling and enabling metrics.
     */
    private LongAdderMetric totalAllocatedPages;

    /** Time interval (in milliseconds) when allocations/evictions are counted to calculate rate. */
    private volatile long rateTimeInterval;

    /** */
    private volatile int subInts;

    /** Page memory implementation. */
    private PageMemory pageMem;

    /** Data region configuration. */
    private DataRegionConfiguration memPlcCfg;

    /** Data region metric provider. */
    private DataRegionMetricsProvider dataRegionMetricsProvider;

    //TODO: See getOrAllocateGroupPageAllocationTracker() method.
    /** Page allocation trackers per cache group. */
    private final Map<Integer, LongAdderMetric> grpAllocationTrackers = new ConcurrentHashMap<>();

    /**
     * Creates data region metric source with given data region parameters.
     *
     * @param name Metric source name.
     * @param ctx KErnal context.
     * @param memPlcCfg Data region configuration.
     * @param dataRegionMetricsProvider Data region metrics provider.
     */
    public DataRegionMetricSource(
            String name,
            GridKernalContext ctx,
            DataRegionConfiguration memPlcCfg,
            DataRegionMetricsProvider dataRegionMetricsProvider
    ) {
        super(name, ctx);

        this.memPlcCfg = memPlcCfg;
        this.dataRegionMetricsProvider = dataRegionMetricsProvider;

        rateTimeInterval = memPlcCfg.getMetricsRateTimeInterval();

        subInts = memPlcCfg.getMetricsSubIntervalCount();

        // Should be initialized always because this counter will have valid value only in this case.
        // Ideally should be introduced to page memory implementation and provide accessors.
        totalAllocatedPages = new LongAdderWithDelegateMetric(
                metricName(name(), "TotalAllocatedPages"),
                this::updateAllocRate,
                "Total number of allocated pages."
        );
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        bldr.addMetric("TotalAllocatedPages", totalAllocatedPages);

        hldr.allocRate = bldr.hitRateMetric("AllocationRate",
                "Allocation rate (pages per second) averaged across rateTimeInternal.",
                60_000,
                5);

        hldr.evictRate = bldr.hitRateMetric("EvictionRate",
                "Eviction rate (pages per second).",
                60_000,
                5);

        hldr.pageReplaceRate = bldr.hitRateMetric("PagesReplaceRate",
                "Rate at which pages in memory are replaced with pages from persistent storage (pages per second).",
                60_000,
                5);

        hldr.pageReplaceAge = bldr.hitRateMetric("PagesReplaceAge",
                "Average age at which pages in memory are replaced with pages from persistent storage (milliseconds).",
                60_000,
                5);

        hldr.largeEntriesPages = bldr.longAdderMetric("LargeEntriesPagesCount",
                "Count of pages that fully ocupied by large entries that go beyond page size");

        hldr.dirtyPages = bldr.longAdderMetric("DirtyPages",
                "Number of pages in memory not yet synchronized with persistent storage.");

        hldr.readPages = bldr.longAdderMetric("PagesRead",
                "Number of pages read from last restart.");

        hldr.writtenPages = bldr.longAdderMetric("PagesWritten",
                "Number of pages written from last restart.");

        hldr.replacedPages = bldr.longAdderMetric("PagesReplaced",
                "Number of pages replaced from last restart.");

        hldr.offHeapSize = bldr.longMetric("OffHeapSize",
                "Offheap size in bytes.");

        hldr.checkpointBufSize = bldr.longMetric("CheckpointBufferSize",
                "Checkpoint buffer size in bytes.");

        bldr.register("EmptyDataPages",
                dataRegionMetricsProvider::emptyDataPages,
                "Calculates empty data pages count for region. It counts only totally free pages that can be reused " +
                        "(e. g. pages that are contained in reuse bucket of free list).");

        bldr.register("PagesFillFactor",
                this::pagesFillFactor,
                "The percentage of the used space.");

        bldr.register("PhysicalMemoryPages",
                this::physicalMemoryPages,
                "Number of pages residing in physical RAM.");

        bldr.register("OffheapUsedSize",
                this::offheapUsedSize,
                "Offheap used size in bytes.");

        bldr.register("TotalAllocatedSize",
                this::totalAllocatedSize,
                "Gets a total size of memory allocated in the data region, in bytes");

        bldr.register("PhysicalMemorySize",
                this::physicalMemorySize,
                "Gets total size of pages loaded to the RAM, in bytes");

        bldr.register("UsedCheckpointBufferSize",
                this::usedCheckpointBufferSize,
                "Gets used checkpoint buffer size in bytes");

        hldr.totalThrottlingTime = bldr.longAdderMetric("TotalThrottlingTime",
                "Total throttling threads time in milliseconds. The Ignite throttles threads that generate " +
                        "dirty pages during the ongoing checkpoint.");
    }

    /**
     * Returns a total number of pages used for storing the data. It includes allocated pages except of empty
     * pages that are not used yet or pages that can be reused.
     * <p>
     * E. g. data region contains 1000 allocated pages, and 200 pages are used to store some data, this
     * metric shows 200 used pages. Then the data was partially deleted and 50 pages were totally freed,
     * hence this metric should show 150 used pages.
     *
     * @return Total number of used pages.
     */
    //TODO: It seems that should be removed and new metric should be added - emptyDataPages.
    public long totalUsedPages() {
        Holder hldr = holder();

        if (hldr != null)
            return totalAllocatedPages.value() - dataRegionMetricsProvider.emptyDataPages();
        else
            return -1;
    }

    /**
     * Returnss pages allocation rate of a memory region.
     *
     * @return Number of allocated pages per second.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public float allocationRate() {
        Holder hldr = holder();

        if (hldr != null)
            return ((float)hldr.allocRate.value() * 1000) / rateTimeInterval;
        else
            return -1;
    }

    /**
     * Returns a total number of allocated pages related to the data region. When persistence is disabled, this
     * metric shows the total number of pages in memory. When persistence is enabled, this metric shows the
     * total number of pages in memory and on disk.
     *
     * @return Total number of allocated pages.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long totalAllocatedPages() {
        Holder hldr = holder();

        return hldr != null ? totalAllocatedPages.value() : -1;
    }

    /**
     * Returns metric implementation that serves as allocated pages tracker.
     */
    //TODO: Bug. All usages should be replaced by instance of DataRegionMetricSource.
    public LongAdderMetric totalAllocatedPagesMetric() {
        Holder hldr = holder();

        if (hldr != null)
            return totalAllocatedPages;
        else
            return NO_OP;
    }

    /**
     * Sets rate time interval.
     *
     * @param rateTimeInterval Time interval (in milliseconds) used to calculate allocation/eviction rate.
     */
    public void rateTimeInterval(long rateTimeInterval) {
        Holder hldr = holder();

        if (hldr != null) {
            this.rateTimeInterval = rateTimeInterval;

            hldr.allocRate.reset(rateTimeInterval, subInts);
            hldr.evictRate.reset(rateTimeInterval, subInts);
            hldr.pageReplaceRate.reset(rateTimeInterval, subInts);
            hldr.pageReplaceAge.reset(rateTimeInterval, subInts);
        }
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

        Holder hldr = holder();

        if (hldr != null) {
            long rateTimeInterval0 = rateTimeInterval;

            if (rateTimeInterval0 / subInts < 10)
                subInts = (int)rateTimeInterval0 / 10;

            hldr.allocRate.reset(rateTimeInterval0, subInts);
            hldr.evictRate.reset(rateTimeInterval0, subInts);
            hldr.pageReplaceRate.reset(rateTimeInterval0, subInts);
            hldr.pageReplaceAge.reset(rateTimeInterval0, subInts);
        }
    }

    /**
     * Returns the percentage of the used space.
     *
     * @return The percentage of the used space.
     */
    public float pagesFillFactor() {
        if (dataRegionMetricsProvider == null)
            return 0;

        Holder hldr = holder();

        if (hldr != null) {
            long freeSpace = dataRegionMetricsProvider.partiallyFilledPagesFreeSpace();

            long totalAllocated = pageMem.pageSize() * totalAllocatedPages.value();

            return totalAllocated != 0 ? (float) (totalAllocated - freeSpace) / totalAllocated : 0f;
        }
        else
            return Float.NaN;
    }

    /**
     * Returns total number of pages currently loaded to the RAM. When persistence is disabled, this metric is equal
     * to {@link #totalAllocatedPages()}.
     *
     * @return Total number of pages loaded to RAM.
     */
    public long physicalMemoryPages() {
        Holder hldr = holder();

        if (hldr != null) {
            if (!memPlcCfg.isPersistenceEnabled())
                return totalAllocatedPages.value();

            assert pageMem != null;

            return pageMem.loadedPages();
        }
        else
            return -1;
    }

    /**
     * Returns total used offheap size in bytes.
     *
     * @return Total used offheap size in bytes.
     */
    //TODO: Should be package-private after removing usage from DataRegionMEtricsImpl.
    public long offheapUsedSize() {
        return pageMem.loadedPages() * pageMem.systemPageSize();
    }

    /**
     * Returns a total size of memory allocated in the data region. When persistence is disabled, this
     * metric shows the total size of pages in memory. When persistence is enabled, this metric shows the
     * total size of pages in memory and on disk.
     *
     * @return Total size of memory allocated, in bytes.
     */
    public long totalAllocatedSize() {
        Holder hldr = holder();

        if (hldr != null) {
            return totalAllocatedPages.value() *
                    (memPlcCfg.isPersistenceEnabled() ? pageMem.pageSize() : pageMem.systemPageSize());
        }
        else
            return -1;
    }

    /**
     * Returns total size of pages loaded to the RAM. When persistence is disabled, this metric is equal
     * to {@link #totalAllocatedSize()}.
     *
     * @return Total size of pages loaded to RAM in bytes.
     */
    //TODO: Should be private aftere removing ussage from DataREgionMetricsImpl.
    public long physicalMemorySize() {
        if (!enabled())
            return -1;

        return physicalMemoryPages() * pageMem.systemPageSize();
    }

    /**
     * Returns used checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long usedCheckpointBufferSize() {
        if (!enabled())
            return -1;

        return usedCheckpointBufferPages() * pageMem.systemPageSize();
    }

    /**
     * Returns used checkpoint buffer size in pages.
     *
     * @return Checkpoint buffer size in pages.
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long usedCheckpointBufferPages() {
        if (!enabled() || !memPlcCfg.isPersistenceEnabled())
            return -1;

        assert pageMem != null;

        return pageMem.checkpointBufferPagesCount();
    }

    /** Increments large entries pages. */
    public void incrementLargeEntriesPages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.largeEntriesPages.increment();
    }

    /** Decrements large entries pages. */
    public void decrementLargeEntriesPages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.largeEntriesPages.decrement();
    }

    /**
     * Updates offheao memory size value.
     *
     * @param size Region size.
     */
    public void updateOffHeapSize(long size) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.offHeapSize.add(size);
    }

    /** Increments eviction reate. */
    public void incrementEvictionRate() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.evictRate.increment();
    }

    /**
     * Sets page memeory implementation.
     *
     * @param pageMem Page memeory implementation.
     */
    //TODO: consider passing this via constructor
    public void pageMemory(PageMemory pageMem) {
        this.pageMem = pageMem;
    }

    /**
     * Adds given value to checkpoint buffer size.
     *
     * @param size Value which will be added.
     */
    public void updateCheckpointBufferSize(long size) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.checkpointBufSize.add(size);
    }

    /**
     * Returns eviction rate of a given memory region.
     *
     * @return Number of evicted pages per second.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public float evictionRate() {
        Holder hldr = holder();

        if (hldr != null)
            return ((float)hldr.evictRate.value() * 1000) / rateTimeInterval;
        else
            return Float.NaN;
    }

    /**
     * Returns percentage of pages that are fully occupied by large entries that go beyond page size. The large entities
     * are split into fragments in a way so that each fragment can fit into a single page.
     *
     * @return Percentage of pages fully occupied by large entities.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed. Could be calculated by
     * external tools.
     */
    @Deprecated
    public float largeEntriesPagesPercentage() {
        Holder hldr = holder();

        if (hldr != null) {
            return totalAllocatedPages.value() != 0 ?
                    (float) hldr.largeEntriesPages.value() / totalAllocatedPages.value() : 0;
        }
        else
            return Float.NaN;
    }

    /**
     * Returns the number of dirty pages (pages which contents is different from the current persistent storage state).
     * This metric is enabled only for Ignite nodes with enabled persistence.
     *
     * @return Current number of dirty pages.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long dirtyPages() {
        Holder hldr = holder();

        if (hldr != null) {
            if (!memPlcCfg.isPersistenceEnabled())
                return 0;

            return hldr.dirtyPages.value();
        }
        else
            return -1;
    }

    /**
     * Returns rate (pages per second) at which pages get replaced with other pages from persistent storage.
     * The rate effectively represents the rate at which pages get 'evicted' in favor of newly needed pages.
     * This metric is enabled only for Ignite nodes with enabled persistence.
     *
     * @return Pages per second replace rate.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public float pagesReplaceRate() {
        Holder hldr = holder();

        if (hldr != null) {
            if (!memPlcCfg.isPersistenceEnabled())
                return 0;

            return ((float)hldr.pageReplaceRate.value() * 1000) / rateTimeInterval;
        }
        else
            return Float.NaN;
    }

    /**
     * Returns average age (in milliseconds) for the pages being replaced from the disk storage.
     * This number effectively represents the average time between the moment when a page is read
     * from the disk and the time when the page is evicted. Note that if a page is never evicted, it does
     * not contribute to this metric.
     * This metric is enabled only for Ignite nodes with enabled persistence.
     *
     * @return Replaced pages age in milliseconds.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed. Could be calculated by
     * external tools.
     */
    @Deprecated
    public float pagesReplaceAge() {
        Holder hldr = holder();

        if (hldr != null) {
            if (!memPlcCfg.isPersistenceEnabled())
                return 0;

            long rep = hldr.pageReplaceRate.value();

            return rep == 0 ? 0 : ((float)hldr.pageReplaceAge.value() / rep);
        }
        else
            return Float.NaN;
    }

    /**
     * Returns number of read pages from last restart.
     *
     * @return The number of read pages from last restart.
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long pagesRead() {
        Holder hldr = holder();

        return hldr != null ? hldr.readPages.value() : -1;
    }

    /**
     * Returns number of written pages from last restart.
     *
     * @return The number of written pages from last restart.
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long pagesWritten() {
        Holder hldr = holder();

        return hldr != null ? hldr.writtenPages.value() : -1;
    }

    /**
     * Returns number of replaced pages from last restart .
     *
     * @return The number of replaced pages from last restart .
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long pagesReplaced() {
        Holder hldr = holder();

        return hldr != null ? hldr.replacedPages.value() : -1;
    }

    /**
     * Returns total offheap size in bytes.
     *
     * @return Total offheap size in bytes.
     */
    //TODO: Should be package-private after removing usage from DataRegionMetricsImpl.
    public long offHeapSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.offHeapSize.value() : -1;
    }

    /**
     * Updates page replace rate metric.
     *
     * @param pageAge Page age.
     */
    public void updatePageReplaceRate(long pageAge) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.pageReplaceRate.increment();

            hldr.pageReplaceAge.add(pageAge);

            hldr.replacedPages.increment();
        }
    }

    /**
     * Updates page read.
     */
    public void onPageRead() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.readPages.increment();
    }

    /**
     * Updates page written.
     */
    public void onPageWritten() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.writtenPages.increment();
    }

    /** Increments dirtyPages counter. */
    public void incrementDirtyPages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.dirtyPages.increment();
    }

    /** Decrements dirtyPages counter. */
    public void decrementDirtyPages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.dirtyPages.decrement();
    }

    /** Resets dirtyPages counter to zero. */
    public void resetDirtyPages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.dirtyPages.reset();
    }

    /**
     * Get or allocate group allocation tracker.
     *
     * @param grpId Cache group ID.
     * @return Group allocation tracker.
     */
    public LongAdderMetric getOrCreateGroupPageAllocationTracker(String pref, int grpId) {
        return grpAllocationTrackers.computeIfAbsent(grpId,
                id -> new LongAdderWithDelegateMetric(
                        pref + "TotalAllocatedPages",
                        totalAllocatedPages::add,
                        "Cache group total allocated pages.")
        );
    }

    /**
     * Returns checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    public long checkpointBufferSize() {
        Holder hldr = holder();

        return hldr != null ? hldr.checkpointBufSize.value() : -1;
    }

    /**
     * Increases pages write throttling time on given value.
     *
     * @param val Time to add to {@code totalThrottlingTime} metric in milliseconds.
     */
    public void addThrottlingTime(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.totalThrottlingTime.add(val);
    }

    /**
     * Returns total time of pages write throttling.
     *
     * @return Total time of pages write throttling.
     */
    public long throttlingTime() {
        Holder hldr = holder();

        if (hldr == null)
            return -1;

        return hldr.totalThrottlingTime.value();
    }

    /** Resets metrics. */
    public void reset() {
        Holder hldr = holder();

        if (hldr != null) {
            totalAllocatedPages.reset();
            grpAllocationTrackers.values().forEach(Metric::reset);
            hldr.largeEntriesPages.reset();
            hldr.dirtyPages.reset();
            hldr.readPages.reset();
            hldr.writtenPages.reset();
            hldr.replacedPages.reset();
            hldr.offHeapSize.reset();
            hldr.checkpointBufSize.reset();
            hldr.allocRate.reset();
            hldr.evictRate.reset();
            hldr.pageReplaceRate.reset();
            hldr.pageReplaceAge.reset();
        }
    }

    /**
     * Updates allocation rate metric.
     *
     * @param delta Delta.
     */
    private void updateAllocRate(long delta) {
        if (delta > 0) {
            Holder hldr = holder();

            if (hldr != null)
                hldr.allocRate.add(delta);
        }
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Allocation rate calculator. */
        private HitRateMetric allocRate;

        /** Counter for number of pages occupied by large entries (one entry is larger than one page). */
        private LongAdderMetric largeEntriesPages;

        /** Counter for number of dirty pages. */
        private LongAdderMetric dirtyPages;

        /** */
        private LongAdderMetric readPages;

        /** */
        private LongAdderMetric writtenPages;

        /** */
        private LongAdderMetric replacedPages;

        /** */
        private AtomicLongMetric offHeapSize;

        /** */
        private AtomicLongMetric checkpointBufSize;

        /** Eviction rate calculator. */
        private HitRateMetric evictRate;

        /** */
        private HitRateMetric pageReplaceRate;

        /** */
        private HitRateMetric pageReplaceAge;

        /** Total throttling threads time in milliseconds. */
        private LongAdderMetric totalThrottlingTime;
    }
}
