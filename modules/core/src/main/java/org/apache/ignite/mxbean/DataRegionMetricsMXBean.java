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
package org.apache.ignite.mxbean;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * This interface defines a JMX view on {@link DataRegionMetrics}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
@MXBeanDescription("MBean that provides access to DataRegionMetrics of a local Apache Ignite node.")
public interface DataRegionMetricsMXBean extends DataRegionMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("A name of a memory region the metrics are collected for.")
    @Override public String getName();

    /**
     * Gets initial memory region size defined by its {@link DataRegionConfiguration}.
     *
     * @return Initial size in MB.
     */
    @MXBeanDescription("Initial memory region size defined by its data region.")
    public int getInitialSize();

    /**
     * Maximum memory region size defined by its {@link DataRegionConfiguration}.
     *
     * @return Maximum size in MB.
     */
    @MXBeanDescription("Maximum memory region size defined by its data region.")
    public int getMaxSize();

    /**
     * A path to the memory-mapped files the memory region defined by {@link DataRegionConfiguration} will be
     * mapped to.
     *
     * @return Path to the memory-mapped files.
     */
    @MXBeanDescription("Path to the memory-mapped files.")
    public String getSwapPath();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of allocated pages.")
    @Override public long getTotalAllocatedPages();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of used pages.")
    @Override public long getTotalUsedPages();

    /** {@inheritDoc} */
    @MXBeanDescription("Allocation rate (pages per second) averaged across rateTimeInternal.")
    @Override public float getAllocationRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Eviction rate (pages per second).")
    @Override public float getEvictionRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Percentage of pages that are fully occupied by large entries that go beyond page size.")
    @Override public float getLargeEntriesPagesPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("The percentage of the used space.")
    @Override public float getPagesFillFactor();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages in memory not yet synchronized with persistent storage.")
    @Override public long getDirtyPages();

    /** {@inheritDoc} */
    @MXBeanDescription("Rate at which pages in memory are replaced with pages from persistent storage (pages per second).")
    @Override public float getPagesReplaceRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Average age at which pages in memory are replaced with pages from persistent storage (milliseconds).")
    @Override public float getPagesReplaceAge();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages residing in physical RAM.")
    @Override public long getPhysicalMemoryPages();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages read from last restart.")
    @Override public long getPagesRead();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages written from last restart.")
    @Override public long getPagesWritten();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages replaced from last restart.")
    @Override public long getPagesReplaced();

    /** {@inheritDoc} */
    @MXBeanDescription("Offheap size in bytes.")
    @Override public long getOffHeapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Offheap used size in bytes.")
    @Override public long getOffheapUsedSize();

    /**
     * Enables memory metrics collection on an Apache Ignite node.
     */
    @MXBeanDescription("Enables memory metrics collection on an Apache Ignite node.")
    public void enableMetrics();

    /**
     * Disables memory metrics collection on an Apache Ignite node.
     */
    @MXBeanDescription("Disables memory metrics collection on an Apache Ignite node.")
    public void disableMetrics();

    /**
     * Sets time interval for {@link #getAllocationRate()} and {@link #getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60 seconds, subsequent calls to {@link #getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @param rateTimeInterval Time interval (in milliseconds) used for allocation and eviction rates calculations.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @MXBeanDescription(
        "Sets time interval for pages allocation and eviction monitoring purposes."
    )
    @Deprecated
    public void rateTimeInterval(
        @MXBeanParameter(name = "rateTimeInterval", description = "Time interval (in milliseconds) to set.")
            long rateTimeInterval
    );

    /**
     * Sets a number of sub-intervals the whole {@link #rateTimeInterval(long)} will be split into to calculate
     * {@link #getAllocationRate()} and {@link #getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link #getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     *
     * @param subInts A number of sub-intervals.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @MXBeanDescription(
        "Sets a number of sub-intervals to calculate allocation and eviction rates metrics."
    )
    @Deprecated
    public void subIntervals(
        @MXBeanParameter(name = "subInts", description = "Number of subintervals to set.") int subInts
    );
}
