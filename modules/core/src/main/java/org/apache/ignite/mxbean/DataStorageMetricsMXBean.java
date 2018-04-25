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

import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.configuration.DataStorageConfiguration;

/**
 * An MX bean allowing to monitor and tune persistence metrics.
 */
public interface DataStorageMetricsMXBean extends DataStorageMetrics {
    /** {@inheritDoc} */
    @MXBeanDescription("Average number of WAL records per second written during the last time interval.")
    @Override float getWalLoggingRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Average number of bytes per second written during the last time interval.")
    @Override float getWalWritingRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Current number of WAL segments in the WAL archive.")
    @Override int getWalArchiveSegments();

    /** {@inheritDoc} */
    @MXBeanDescription("Average WAL fsync duration in microseconds over the last time interval.")
    @Override float getWalFsyncTimeAverage();

    /** {@inheritDoc} */
    @MXBeanDescription("WAL buffer poll spins number over the last time interval.")
    @Override long getWalBuffPollSpinsRate();

    /** {@inheritDoc} */
    @MXBeanDescription("Total size in bytes for storage wal files.")
    @Override long getWalTotalSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Time of the last WAL segment rollover.")
    @Override long getWalLastRollOverTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Total checkpoint time from last restart.")
    @Override long getCheckpointTotalTime();

    /** {@inheritDoc} */
    @MXBeanDescription("Duration of the last checkpoint in milliseconds.")
    @Override long getLastCheckpointDuration();

    /** {@inheritDoc} */
    @MXBeanDescription("Duration of the checkpoint lock wait in milliseconds.")
    @Override long getLastCheckpointLockWaitDuration();

    /** {@inheritDoc} */
    @MXBeanDescription("Duration of the checkpoint mark in milliseconds.")
    @Override long getLastCheckpointMarkDuration();

    /** {@inheritDoc} */
    @MXBeanDescription("Duration of the checkpoint pages write in milliseconds.")
    @Override long getLastCheckpointPagesWriteDuration();

    /** {@inheritDoc} */
    @MXBeanDescription("Duration of the sync phase of the last checkpoint in milliseconds.")
    @Override long getLastCheckpointFsyncDuration();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of pages written during the last checkpoint.")
    @Override long getLastCheckpointTotalPagesNumber();

    /** {@inheritDoc} */
    @MXBeanDescription("Total number of data pages written during the last checkpoint.")
    @Override long getLastCheckpointDataPagesNumber();

    /** {@inheritDoc} */
    @MXBeanDescription("Number of pages copied to a temporary checkpoint buffer during the last checkpoint.")
    @Override long getLastCheckpointCopiedOnWritePagesNumber();

    /** {@inheritDoc} */
    @MXBeanDescription("Total dirty pages for the next checkpoint.")
    @Override long getDirtyPages();

    /** {@inheritDoc} */
    @MXBeanDescription("The number of read pages from last restart.")
    @Override long getPagesRead();

    /** {@inheritDoc} */
    @MXBeanDescription("The number of written pages from last restart.")
    @Override long getPagesWritten();

    /** {@inheritDoc} */
    @MXBeanDescription("The number of replaced pages from last restart.")
    @Override long getPagesReplaced();

    /** {@inheritDoc} */
    @MXBeanDescription("Total offheap size in bytes.")
    @Override long getOffHeapSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Total used offheap size in bytes.")
    @Override long getOffheapUsedSize();

    /** {@inheritDoc} */
    @MXBeanDescription("Total size of memory allocated in bytes.")
    @Override long getTotalAllocatedSize();

    /**
     * Enables persistence metrics collection on an Apache Ignite node.
     */
    @MXBeanDescription("Enables persistence metrics collection on an Apache Ignite node.")
    public void enableMetrics();

    /**
     * Disables persistence metrics collection on an Apache Ignite node.
     */
    @MXBeanDescription("Disables persistence metrics collection on an Apache Ignite node.")
    public void disableMetrics();

    /**
     * Sets time interval for rate-based metrics. Identical to setting
     * {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)} configuration property.
     *
     * @param rateTimeInterval Time interval (in milliseconds) used for allocation and eviction rates calculations.
     */
    @MXBeanDescription(
        "Sets time interval for pages allocation and eviction monitoring purposes."
    )
    @MXBeanParametersNames(
        "rateTimeInterval"
    )
    @MXBeanParametersDescriptions(
        "Time interval (in milliseconds) to set."
    )
    public void rateTimeInterval(long rateTimeInterval);

    /**
     * Sets a number of sub-intervals the whole {@link #rateTimeInterval(long)} will be split into to calculate
     * rate-based metrics. Identical to setting {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)} configuration
     * property.
     *
     * @param subInts A number of sub-intervals.
     */
    @MXBeanDescription(
        "Sets a number of sub-intervals to calculate allocation and eviction rates metrics."
    )
    @MXBeanParametersNames(
        "subInts"
    )
    @MXBeanParametersDescriptions(
        "Number of subintervals to set."
    )
    public void subIntervals(int subInts);
}
