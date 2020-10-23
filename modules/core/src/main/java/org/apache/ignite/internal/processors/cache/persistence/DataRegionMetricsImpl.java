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

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.sources.DataRegionMetricSource;
import org.apache.ignite.mxbean.MetricsMxBean;

/**
 * @deprecated Should be removed in Apache Ignite 3.0. Add new metrics to {@link DataRegionMetricSource}.
 */
@Deprecated
public class DataRegionMetricsImpl implements DataRegionMetrics {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Metric source. */
    private final DataRegionMetricSource metricSrc;

    /** Data region name. */
    private final String name;

    /**
     * Page size.
     *
     * @deprecated Isn't metric at all. Constant and the same for all data regions.
     */
    @Deprecated
    private final int pageSize;

    /**
     * @param metricSrc Metric source.
     */
    public DataRegionMetricsImpl(GridKernalContext ctx, String regionName, DataRegionMetricSource metricSrc, int pageSize) {
        this.ctx = ctx;
        this.name = regionName;
        this.metricSrc = metricSrc;
        this.pageSize = pageSize;

        //TODO: Move to metric source
/*
        mreg.longMetric("InitialSize", "Initial memory region size in bytes defined by its data region.")
                .value(memPlcCfg.getInitialSize());

        mreg.longMetric("MaxSize", "Maximum memory region size in bytes defined by its data region.")
                .value(memPlcCfg.getMaxSize());
*/

    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return metricSrc.totalAllocatedPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return metricSrc.totalUsedPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return metricSrc.totalAllocatedSize();
    }

    /** {@inheritDoc}*/
    @Override public float getAllocationRate() {
        return metricSrc.allocationRate();
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return metricSrc.evictionRate();
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        return metricSrc.largeEntriesPagesPercentage();
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        return metricSrc.pagesFillFactor();
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return metricSrc.dirtyPages();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        return metricSrc.pagesReplaceRate();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        return metricSrc.pagesReplaceAge();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        return metricSrc.physicalMemoryPages();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemorySize() {
        return metricSrc.physicalMemorySize();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        return metricSrc.usedCheckpointBufferPages();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return metricSrc.usedCheckpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        return metricSrc.usedCheckpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        return metricSrc.pagesRead();
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getPagesWritten() {
        return metricSrc.pagesWritten();
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getPagesReplaced() {
        return metricSrc.pagesReplaced();
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getOffHeapSize() {
        return metricSrc.offHeapSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override public long getOffheapUsedSize() {
        return metricSrc.offheapUsedSize();
    }

    /**
     * @param size Region size.
     */
    public void updateOffHeapSize(long size) {
        metricSrc.updateOffHeapSize(size);
    }

    /**
     * @param size Checkpoint buffer size.
     */
    public void updateCheckpointBufferSize(long size) {
        metricSrc.updateCheckpointBufferSize(size);
    }

    /**
     * Updates pageReplaceRate metric.
     */
    public void updatePageReplaceRate(long pageAge) {
        metricSrc.updatePageReplaceRate(pageAge);
    }

    /**
     * Updates page read.
     */
    public void onPageRead() {
        metricSrc.onPageRead();
    }

    /**
     * Updates page written.
     */
    public void onPageWritten() {
        metricSrc.onPageWritten();
    }

    /**
     * Increments dirtyPages counter.
     */
    public void incrementDirtyPages() {
        metricSrc.incrementDirtyPages();
    }

    /**
     * Decrements dirtyPages counter.
     */
    public void decrementDirtyPages() {
        metricSrc.decrementDirtyPages();
    }

    /**
     * Resets dirtyPages counter to zero.
     */
    public void resetDirtyPages() {
        metricSrc.resetDirtyPages();
    }

    /**
     *
     */
    public LongAdderMetric totalAllocatedPages() {
        return metricSrc.totalAllocatedPagesMetric();
    }

    /**
     * Updates eviction rate metric.
     */
    public void updateEvictionRate() {
        metricSrc.incrementEvictionRate();
    }

    /**
     *
     */
    public void incrementLargeEntriesPages() {
        metricSrc.incrementLargeEntriesPages();
    }

    /**
     *
     */
    public void decrementLargeEntriesPages() {
        metricSrc.decrementLargeEntriesPages();
    }

    /**
     * Enable metrics.
     */
    public void enableMetrics() {
        ctx.metric().enableMetrics(metricSrc);
    }

    /**
     * Disable metrics.
     */
    public void disableMetrics() {
        ctx.metric().disableMetrics(metricSrc);
    }

    /**
     * @param rateTimeInterval Time interval (in milliseconds) used to calculate allocation/eviction rate.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void rateTimeInterval(long rateTimeInterval) {
        metricSrc.rateTimeInterval(rateTimeInterval);
    }

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate.
     *
     * @param subInts Number of subintervals.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public void subIntervals(int subInts) {
        metricSrc.subIntervals(subInts);
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        metricSrc.reset();
    }

    /** @param time Time to add to {@code totalThrottlingTime} metric in milliseconds. */
    public void addThrottlingTime(long time) {
        metricSrc.addThrottlingTime(time);
    }
}
