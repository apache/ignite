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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsProvider;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetricsImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.HitRateMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderWithDelegateMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.metric.impl.PeriodicHistogramMetricImpl;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.mxbean.MetricsMxBean;
import org.apache.ignite.spi.systemview.view.PagesTimestampHistogramView;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 *
 */
public class DataRegionMetricsImpl implements DataRegionMetrics {
    /**
     * {@link LongAdderWithDelegateMetric.Delegate} that forwards all calls to another {@link LongAdderMetric}.
     */
    private static final class LongAdderMetricDelegate implements LongAdderWithDelegateMetric.Delegate {
        /** */
        private final LongAdderMetric delegate;

        /** */
        LongAdderMetricDelegate(LongAdderMetric delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void increment() {
            delegate.increment();
        }

        /** {@inheritDoc} */
        @Override public void decrement() {
            delegate.decrement();
        }

        /** {@inheritDoc} */
        @Override public void add(long x) {
            delegate.add(x);
        }
    }

    /**
     * Factory method for {@link LongAdderMetricDelegate}.
     */
    private static LongAdderMetricDelegate delegate(LongAdderMetric delegate) {
        return new LongAdderMetricDelegate(delegate);
    }

    /**
     * Data region metrics prefix.
     * Full name will contain {@link DataRegionConfiguration#getName()} also.
     * {@code "io.dataregion.default"}, for example.
     */
    public static final String DATAREGION_METRICS_PREFIX = MetricUtils.metricName("io", "dataregion");

    /** */
    private final DataRegionMetricsProvider dataRegionMetricsProvider;

    /**
     * Cache group ID -> Cache group memory page metrics.
     * <p>
     * This is effectively a copy-on-write hash map, all write operations should create a new copy under the
     * {@link #cacheGrpMetricsLock} lock.
     */
    private volatile IntMap<PageMetrics> cacheGrpMetrics = new IntHashMap<>();

    /** Lock for the {@link #cacheGrpMetrics} field. */
    private final Object cacheGrpMetricsLock = new Object();

    /** Memory page metrics for the whole region. */
    private final PageMetrics dataRegionPageMetrics;

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final LongAdderMetric largeEntriesPages;

    /** Counter for number of dirty pages. */
    private final LongAdderMetric dirtyPages;

    /** */
    private final LongAdderMetric readPages;

    /** */
    private final LongAdderMetric writtenPages;

    /** */
    private final LongAdderMetric replacedPages;

    /** */
    private final AtomicLongMetric offHeapSize;

    /** */
    private final AtomicLongMetric checkpointBufSize;

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private boolean persistenceEnabled;

    /** */
    private final int subInts;

    /** Allocation rate calculator. */
    private final HitRateMetric allocRate;

    /** Eviction rate calculator. */
    private final HitRateMetric evictRate;

    /** */
    private final HitRateMetric pageReplaceRate;

    /** */
    private final HitRateMetric pageReplaceAge;

    /** Total throttling threads time in milliseconds. */
    private final LongAdderMetric totalThrottlingTime;

    /** */
    private final DataRegionConfiguration dataRegionCfg;

    /** */
    @Nullable
    private PageMemory pageMem;

    /** */
    private final GridKernalContext kernalCtx;

    /** Time interval (in milliseconds) when allocations/evictions are counted to calculate rate. */
    private volatile long rateTimeInterval;

    /** Histogram of cold/hot pages. */
    @Nullable
    private final PeriodicHistogramMetricImpl pageTsHistogram;

    /**
     * Same as {@link #DataRegionMetricsImpl(DataRegionConfiguration, GridKernalContext, DataRegionMetricsProvider)}
     * but uses a no-op implementation for the {@link DataRegionMetricsProvider}.
     */
    @TestOnly
    public DataRegionMetricsImpl(DataRegionConfiguration dataRegionCfg, GridKernalContext kernalCtx) {
        this(dataRegionCfg, kernalCtx, new DataRegionMetricsProvider() {
            @Override public long partiallyFilledPagesFreeSpace() {
                return 0;
            }

            @Override public long emptyDataPages() {
                return 0;
            }
        });
    }

    /**
     * @param dataRegionCfg DataRegionConfiguration.
     * @param kernalCtx Kernal context.
     * @param dataRegionMetricsProvider Data region metrics provider.
     */
    public DataRegionMetricsImpl(
        DataRegionConfiguration dataRegionCfg,
        GridKernalContext kernalCtx,
        DataRegionMetricsProvider dataRegionMetricsProvider
    ) {
        this.dataRegionCfg = dataRegionCfg;
        this.dataRegionMetricsProvider = dataRegionMetricsProvider;
        this.kernalCtx = kernalCtx;

        metricsEnabled = dataRegionCfg.isMetricsEnabled();

        persistenceEnabled = dataRegionCfg.isPersistenceEnabled();

        rateTimeInterval = dataRegionCfg.getMetricsRateTimeInterval();

        subInts = dataRegionCfg.getMetricsSubIntervalCount();

        MetricRegistry mreg = metricRegistry();

        allocRate = mreg.hitRateMetric("AllocationRate",
            "Allocation rate (pages per second) averaged across rateTimeInterval.",
            60_000,
            5);

        evictRate = mreg.hitRateMetric("EvictionRate",
            "Eviction rate (pages per second).",
            60_000,
            5);

        pageReplaceRate = mreg.hitRateMetric("PagesReplaceRate",
            "Rate at which pages in memory are replaced with pages from persistent storage (pages per second).",
            60_000,
            5);

        pageReplaceAge = mreg.hitRateMetric("PagesReplaceAge",
            "Average age at which pages in memory are replaced with pages from persistent storage (milliseconds).",
            60_000,
            5);

        largeEntriesPages = mreg.longAdderMetric("LargeEntriesPagesCount",
            "Count of pages that fully ocupied by large entries that go beyond page size");

        dirtyPages = mreg.longAdderMetric("DirtyPages",
            "Number of pages in memory not yet synchronized with persistent storage.");

        readPages = mreg.longAdderMetric("PagesRead",
            "Number of pages read from last restart.");

        writtenPages = mreg.longAdderMetric("PagesWritten",
            "Number of pages written from last restart.");

        replacedPages = mreg.longAdderMetric("PagesReplaced",
            "Number of pages replaced from last restart.");

        offHeapSize = mreg.longMetric("OffHeapSize",
            "Offheap size in bytes.");

        checkpointBufSize = mreg.longMetric("CheckpointBufferSize",
            "Checkpoint buffer size in bytes.");

        mreg.register("EmptyDataPages",
            dataRegionMetricsProvider::emptyDataPages,
            "Calculates empty data pages count for region. It counts only totally free pages that can be reused " +
                "(e. g. pages that are contained in reuse bucket of free list).");

        totalThrottlingTime = mreg.longAdderMetric("TotalThrottlingTime",
            "Total throttling threads time in milliseconds. The Ignite throttles threads that generate " +
                "dirty pages during the ongoing checkpoint.");

        mreg.longMetric("InitialSize", "Initial memory region size in bytes defined by its data region.")
            .value(dataRegionCfg.getInitialSize());

        mreg.longMetric("MaxSize", "Maximum memory region size in bytes defined by its data region.")
            .value(dataRegionCfg.getMaxSize());

        if (persistenceEnabled) {
            // Reserve 1 sec, page ts can be slightly lower than currentTimeMillis, due to applied to ts mask. This
            // reservation mainly affects only tests (we can check buckets more predictevely).
            long startTs = U.currentTimeMillis() - 1000L;
            String name = MetricUtils.metricName(mreg.name(), "PageTimestampHistogram");
            String desc = "Histogram of pages last access time";

            pageTsHistogram = new PeriodicHistogramMetricImpl(startTs, name, desc);

            mreg.register(pageTsHistogram);
        }
        else
            pageTsHistogram = null;

        dataRegionPageMetrics = PageMetricsImpl.builder(mreg)
            .totalPagesCallback(new LongAdderWithDelegateMetric.Delegate() {
                @Override public void increment() {
                    add(1);
                }

                @Override public void add(long x) {
                    if (metricsEnabled && x > 0)
                        allocRate.add(x);
                }

                @Override public void decrement() {
                }
            })
            .build();
    }

    /**
     * Retrieves the {@link MetricRegistry} for this data region.
     */
    private MetricRegistry metricRegistry() {
        String registryName = MetricUtils.metricName(DATAREGION_METRICS_PREFIX, dataRegionCfg.getName());
        return kernalCtx.metric().registry(registryName);
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return U.maskName(dataRegionCfg.getName());
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return dataRegionPageMetrics.totalPages().value();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return getTotalAllocatedPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return getTotalAllocatedPages() - dataRegionMetricsProvider.emptyDataPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedSize() {
        return getTotalUsedPages() * pageMem.systemPageSize();
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)allocRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        if (!metricsEnabled)
            return 0;

        return ((float)evictRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (!metricsEnabled)
            return 0;

        long totalAllocatedPages = getTotalAllocatedPages();
        return totalAllocatedPages != 0 ? (float)largeEntriesPages.value() / totalAllocatedPages : 0;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        if (!metricsEnabled)
            return 0;

        long totalUsedSize = getTotalUsedSize();

        if (totalUsedSize == 0)
            return 0;

        long freeSpaceInPages = dataRegionMetricsProvider.partiallyFilledPagesFreeSpace();

        return (float)(totalUsedSize - freeSpaceInPages) / totalUsedSize;
    }

    /**
     * Calculates the number of bytes, occupied by data. Unlike {@link #getTotalUsedSize} it also takes into account the
     * empty space in non-empty pages.
     */
    private long getSizeUsedByData() {
        // Total amount of bytes occupied by all pages.
        long totalSpace = getTotalAllocatedSize();

        // Amount of free bytes in fragmented data pages.
        long partiallyFreeSpace = dataRegionMetricsProvider.partiallyFilledPagesFreeSpace();

        // Amount of bytes in empty data pages.
        long emptySpace = dataRegionMetricsProvider.emptyDataPages() * pageMem.systemPageSize();

        return totalSpace - partiallyFreeSpace - emptySpace;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return dirtyPages.value();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        return ((float)pageReplaceRate.value() * 1000) / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        if (!metricsEnabled || !persistenceEnabled)
            return 0;

        long rep = pageReplaceRate.value();

        return rep == 0 ? 0 : ((float)pageReplaceAge.value() / rep);
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        if (!persistenceEnabled)
            return getTotalAllocatedPages();

        if (!metricsEnabled)
            return 0;

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

        return checkpointBufSize.value();
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        if (!metricsEnabled)
            return 0;

        return pageMem.pageSize();
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        if (!metricsEnabled)
            return 0;

        return readPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        if (!metricsEnabled)
            return 0;

        return writtenPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        if (!metricsEnabled)
            return 0;

        return replacedPages.value();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return offHeapSize.value();
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
        this.checkpointBufSize.add(size);
    }

    /**
     * Memory page metrics associated with the data region.
     */
    public PageMetrics pageMetrics() {
        return dataRegionPageMetrics;
    }

    /**
     * Updates pageReplaceRate metric.
     */
    public void updatePageReplaceRate(long pageAge) {
        if (metricsEnabled) {
            pageReplaceRate.increment();

            pageReplaceAge.add(pageAge);

            replacedPages.increment();
        }
    }

    /**
     * Updates page read.
     */
    public void onPageRead() {
        if (metricsEnabled)
            readPages.increment();
    }

    /**
     * Updates page written.
     */
    public void onPageWritten() {
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

    /**
     * Returns memory page metrics associated with the given cache group.
     */
    public PageMetrics cacheGrpPageMetrics(int cacheGrpId) {
        PageMetrics pageMetrics = cacheGrpMetrics.get(cacheGrpId);

        if (pageMetrics != null)
            return pageMetrics;

        synchronized (cacheGrpMetricsLock) {
            IntMap<PageMetrics> locCacheGrpMetrics = cacheGrpMetrics;

            // double check
            PageMetrics doubleCheckPageMetrics = locCacheGrpMetrics.get(cacheGrpId);
            if (doubleCheckPageMetrics != null)
                return doubleCheckPageMetrics;

            IntMap<PageMetrics> copy = new IntHashMap<>(locCacheGrpMetrics);

            PageMetrics newMetrics = Optional.of(kernalCtx)
                // both cache and group descriptor can be null
                .map(GridKernalContext::cache)
                .map(cache -> cache.cacheGroupDescriptors().get(cacheGrpId))
                .map(decs -> createCacheGrpPageMetrics(decs.cacheOrGroupName()))
                // return region-wide metrics for a non-existent cache group ID. This is needed in some scenarios
                // when per-group data structures are used on a per-region basis and in tests.
                .orElse(dataRegionPageMetrics);

            copy.put(cacheGrpId, newMetrics);

            cacheGrpMetrics = copy;

            return newMetrics;
        }
    }

    /**
     * Creates memory page metrics container associated with the given cache group.
     */
    private PageMetrics createCacheGrpPageMetrics(String cacheGrpName) {
        String registryName = MetricUtils.cacheGroupMetricsRegistryName(cacheGrpName);
        MetricRegistry registry = kernalCtx.metric().registry(registryName);

        return PageMetricsImpl.builder(registry)
            .totalPagesCallback(delegate(dataRegionPageMetrics.totalPages()))
            .indexPagesCallback(delegate(dataRegionPageMetrics.indexPages()))
            .build();
    }

    /**
     * Removes all memory page metrics associated with the given cache group.
     */
    public void removeCacheGrpPageMetrics(Integer grpId) {
        PageMetrics rmvMetrics;

        synchronized (cacheGrpMetricsLock) {
            IntMap<PageMetrics> copy = new IntHashMap<>(cacheGrpMetrics);

            rmvMetrics = copy.remove(grpId);

            cacheGrpMetrics = copy;
        }

        // we don't decrease the total pages counter for historical reasons
        // (it hasn't been done in previous implementations)
        if (rmvMetrics != null)
            dataRegionPageMetrics.indexPages().add(-rmvMetrics.indexPages().value());
    }

    /**
     * Updates eviction rate metric.
     */
    public void updateEvictionRate() {
        if (metricsEnabled)
            evictRate.increment();
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

        if (pageTsHistogram != null)
            pageTsHistogram.reset(getPhysicalMemoryPages());
    }

    /**
     * Disable metrics.
     */
    public void disableMetrics() {
        metricsEnabled = false;

        if (pageTsHistogram != null)
            pageTsHistogram.reset(0);
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

        MetricRegistry mreg = metricRegistry();

        mreg.register("PagesFillFactor",
            this::getPagesFillFactor,
            "Returns the ratio of space occupied by user and system data to the size of all pages that contain " +
                "this data");

        mreg.register("SizeUsedByData",
            this::getSizeUsedByData,
            "Returns the number of bytes, occupied by data. Similar to TotalUsedSize, but it also takes into " +
                "account the empty space in non-empty pages");

        mreg.register("PhysicalMemoryPages",
            this::getPhysicalMemoryPages,
            "Number of pages residing in physical RAM");

        mreg.register("OffheapUsedSize",
            this::getOffheapUsedSize,
            "Offheap used size in bytes");

        // TotalAllocatedPages metrics is registered by PageMetrics

        mreg.register("TotalAllocatedSize",
            this::getTotalAllocatedSize,
            "Gets a total size of memory allocated in the data region, in bytes");

        mreg.register("TotalUsedPages",
            this::getTotalUsedPages,
            "Gets an amount of non-empty pages allocated in the data region");

        mreg.register("TotalUsedSize",
            this::getTotalUsedSize,
            "Gets an amount of bytes, occupied by non-empty pages allocated in the data region");

        mreg.register("PhysicalMemorySize",
            this::getPhysicalMemorySize,
            "Gets total size of pages loaded to the RAM, in bytes");

        mreg.register("UsedCheckpointBufferSize",
            this::getUsedCheckpointBufferSize,
            "Gets used checkpoint buffer size in bytes");
    }

    /**
     * @param rateTimeInterval Time interval (in milliseconds) used to calculate allocation/eviction rate.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void rateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        allocRate.reset(rateTimeInterval, subInts);
        evictRate.reset(rateTimeInterval, subInts);
        pageReplaceRate.reset(rateTimeInterval, subInts);
        pageReplaceAge.reset(rateTimeInterval, subInts);
    }

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate.
     *
     * @param subInts Number of subintervals.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void subIntervals(int subInts) {
        assert subInts > 0;

        if (this.subInts == subInts)
            return;

        if (rateTimeInterval / subInts < 10)
            subInts = (int)rateTimeInterval / 10;

        allocRate.reset(rateTimeInterval, subInts);
        evictRate.reset(rateTimeInterval, subInts);
        pageReplaceRate.reset(rateTimeInterval, subInts);
        pageReplaceAge.reset(rateTimeInterval, subInts);
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        largeEntriesPages.reset();
        dirtyPages.reset();
        readPages.reset();
        writtenPages.reset();
        replacedPages.reset();
        offHeapSize.reset();
        checkpointBufSize.reset();
        allocRate.reset();
        evictRate.reset();
        pageReplaceRate.reset();
        pageReplaceAge.reset();

        dataRegionPageMetrics.reset();

        for (PageMetrics metrics : cacheGrpMetrics.values())
            metrics.reset();
    }

    /** Removes all metric for data region. */
    public void remove() {
        kernalCtx.metric().remove(metricRegistry().name(), false);
    }

    /** @param time Time to add to {@code totalThrottlingTime} metric in milliseconds. */
    public void addThrottlingTime(long time) {
        if (kernalCtx.performanceStatistics().enabled())
            kernalCtx.performanceStatistics().pagesWriteThrottle(U.currentTimeMillis(), time);

        if (metricsEnabled)
            totalThrottlingTime.add(time);
    }

    /**
     * Increment count of pages with given last access time.
     *
     * @param ts Last access timestamp.
     */
    public void incrementPagesWithTimestamp(long ts) {
        if (metricsEnabled && pageTsHistogram != null)
            pageTsHistogram.increment(ts);
    }

    /**
     * Decrement count of pages with given last access time.
     *
     * @param ts Last access timestamp.
     */
    public void decrementPagesWithTimestamp(long ts) {
        if (metricsEnabled && pageTsHistogram != null)
            pageTsHistogram.decrement(ts);
    }

    /**
     * Creates pages timestamp histogram view.
     */
    public Collection<PagesTimestampHistogramView> pagesTimestampHistogramView() {
        if (!metricsEnabled || pageTsHistogram == null)
            return Collections.emptyList();

        IgniteBiTuple<long[], long[]> hist = pageTsHistogram.histogram();

        long[] bounds = hist.get1();
        long[] vals = hist.get2();

        List<PagesTimestampHistogramView> list = new ArrayList<>(vals.length);

        for (int i = 0; i < vals.length - 1; i++)
            list.add(new PagesTimestampHistogramView(getName(), bounds[i], bounds[i + 1], vals[i]));

        list.add(new PagesTimestampHistogramView(getName(), bounds[vals.length - 1],
            U.currentTimeMillis(), vals[vals.length - 1]));

        return list;
    }
}
