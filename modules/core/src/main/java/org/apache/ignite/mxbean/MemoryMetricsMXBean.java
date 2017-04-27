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

/**
 * This interface defines JMX view on {@link MemoryMetrics}.
 */
@MXBeanDescription("MBean that provides access to MemoryMetrics of current Ignite node.")
public interface MemoryMetricsMXBean extends MemoryMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("Name of MemoryPolicy metrics are collected for.")
    @Override public String getName();

    /**
     * Initial size configured for MemoryPolicy on local node.
     *
     * @return Initial size in MB.
     */
    @MXBeanDescription("Initial size configured for MemoryPolicy on local node.")
    public int getInitialSize();

    /**
     * Maximum size configured for MemoryPolicy on local node.
     *
     * @return Maximum size in MB.
     */
    @MXBeanDescription("Maximum size configured for MemoryPolicy on local node.")
    public int getMaxSize();

    /**
     * Path from MemoryPolicy configuration to directory where memory-mapped files used for swap are created.
     * Depending on configuration may be absolute or relative; in the latter case it is relative to IGNITE_HOME.
     *
     * @return path to directory with memory-mapped files.
     */
    @MXBeanDescription("Path to directory with memory-mapped files.")
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
    @MXBeanDescription("Percentage of pages fully occupied by large entities' fragments.")
    @Override public float getLargeEntriesPagesPercentage();

    /** {@inheritDoc} */
    @MXBeanDescription("Pages fill factor: size of all entries in cache over size of all allocated pages.")
    @Override public float getPagesFillFactor();

    /**
     * Enables collecting memory metrics on local node.
     */
    @MXBeanDescription("Enables collecting memory metrics on local node.")
    public void enableMetrics();

    /**
     * Disables collecting memory metrics on local node.
     */
    @MXBeanDescription("Disables collecting memory metrics on local node.")
    public void disableMetrics();

    /**
     * Sets interval of time (in seconds) to monitor allocation rate.
     *
     * E.g. after setting rateTimeInterval to 60 seconds subsequent calls to {@link #getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @param rateTimeInterval Time interval used to calculate allocation/eviction rate.
     */
    @MXBeanDescription(
        "Sets time interval average allocation rate (pages per second) is calculated over."
    )
    @MXBeanParametersNames(
        "rateTimeInterval"
    )
    @MXBeanParametersDescriptions(
        "Time interval (in seconds) to set."
    )
    public void rateTimeInterval(int rateTimeInterval);

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate,
     * 5 by default.
     * Setting it to bigger number allows more precise calculation and smaller drops of allocationRate metric
     * when next subinterval has to be recycled but introduces bigger calculation overhead.
     *
     * @param subInts Number of subintervals.
     */
    @MXBeanDescription(
        "Sets number of subintervals to calculate allocationRate metrics."
    )
    @MXBeanParametersNames(
        "subInts"
    )
    @MXBeanParametersDescriptions(
        "Number of subintervals to set."
    )
    public void subIntervals(int subInts);
}
