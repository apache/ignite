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
    @MXBeanDescription("Name of PageMemory metrics are collected for.")
    @Override public String getName();

    /** {@inheritDoc} */
    @MXBeanDescription("Size of PageMemory in MBytes.")
    @Override public int getSize();

    /** {@inheritDoc} */
    @MXBeanDescription("File path of memory-mapped swap file.")
    @Override public String getSwapFilePath();

    /** {@inheritDoc} */
    @MXBeanDescription("Enables metrics gathering.")
    @Override public void enableMetrics();

    /** {@inheritDoc} */
    @MXBeanDescription("Disables metrics gathering.")
    @Override public void disableMetrics();

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

    /** {@inheritDoc} */
    @MXBeanDescription(
            "Sets time interval average allocation rate (pages per second) is calculated over."
    )
    @MXBeanParametersNames(
            "rateTimeInterval"
    )
    @MXBeanParametersDescriptions(
            "Time interval (in seconds) to set."
    )
    @Override public void rateTimeInterval(int rateTimeInterval);

    /** {@inheritDoc} */
    @MXBeanDescription(
            "Sets number of subintervals to calculate allocationRate metrics."
    )
    @MXBeanParametersNames(
            "subInts"
    )
    @MXBeanParametersDescriptions(
            "Number of subintervals to set."
    )
    @Override public void subIntervals(int subInts);
}
