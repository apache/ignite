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

import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;

/**
 * This interface defines a JMX view on {@link MemoryMetrics}.
 * @deprecated Part of old API. Metrics are accessible through {@link DataRegionMetricsMXBean}.
 */
@MXBeanDescription("MBean that provides access to MemoryMetrics of a local Apache Ignite node.")
@Deprecated
public interface MemoryMetricsMXBean extends MemoryMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("A name of a memory region the metrics are collected for.")
    @Override public String getName();

    /**
     * Gets initial memory region size defined by its {@link MemoryPolicyConfiguration}.
     *
     * @return Initial size in MB.
     */
    @MXBeanDescription("Initial memory region size defined by its memory policy.")
    public int getInitialSize();

    /**
     * Maximum memory region size defined by its {@link MemoryPolicyConfiguration}.
     *
     * @return Maximum size in MB.
     */
    @MXBeanDescription("Maximum memory region size defined by its memory policy.")
    public int getMaxSize();

    /**
     * A path to the memory-mapped files the memory region defined by {@link MemoryPolicyConfiguration} will be
     * mapped to.
     *
     * @return Path to the memory-mapped files.
     */
    @MXBeanDescription("Path to the memory-mapped files.")
    public String getSwapFilePath();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of allocated pages.")
    @Override public long getTotalAllocatedPages();

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
    @MXBeanDescription("Percentage of space that is still free and can be filled in.")
    @Override public float getPagesFillFactor();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages in memory not yet synchronized with persistent storage.")
    @Override public long getDirtyPages();

    /** {@inheritDoc} */
    @MXBeanDescription("Rate at which pages in memory are replaced with pages from persistent storage (pages per second).")
    @Override public float getPagesReplaceRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages residing in physical RAM.")
    @Override public long getPhysicalMemoryPages();

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
     */
    @MXBeanDescription(
        "Sets time interval for pages allocation and eviction monitoring purposes."
    )
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
     */
    @MXBeanDescription(
        "Sets a number of sub-intervals to calculate allocation and eviction rates metrics."
    )
    public void subIntervals(
        @MXBeanParameter(name = "subInts", description = "Number of subintervals to set.") int subInts
    );
}
