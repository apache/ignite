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
package org.apache.ignite;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * Data storage metrics are used to obtain statistics on persistent store and whole data storage.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public interface DataStorageMetrics {
    /**
     * Gets the average number of WAL records per second written during the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     */
    public float getWalLoggingRate();

    /**
     * Gets the average number of bytes per second written during the last time interval.
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     */
    public float getWalWritingRate();

    /**
     * Gets the current number of WAL segments in the WAL archive.
     */
    public int getWalArchiveSegments();

    /**
     * Gets the average WAL fsync duration in microseconds over the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     */
    public float getWalFsyncTimeAverage();

    /**
     * Returns WAL buffer poll spins number over the last time interval.
     * <p>
     * The length of time interval is configured via {@link DataStorageConfiguration#setMetricsRateTimeInterval(long)}
     * configuration property.
     * The number of subintervals is configured via {@link DataStorageConfiguration#setMetricsSubIntervalCount(int)}
     * configuration property.
     */
    public long getWalBuffPollSpinsRate();

    /**
     * Total size in bytes for storage wal files.
     *
     * @return Total size in bytes for storage wal files.
     */
    public long getWalTotalSize();

    /**
     * Time of the last WAL segment rollover.
     *
     * @return  Time of the last WAL segment rollover.
     */
    public long getWalLastRollOverTime();

    /**
     * Total checkpoint time from last restart.
     *
     * @return  Total checkpoint time from last restart.
     */
    public long getCheckpointTotalTime();

    /**
     * Gets the duration of the last checkpoint in milliseconds.
     *
     * @return Total checkpoint duration in milliseconds.
     */
    public long getLastCheckpointDuration();

    /**
     * Gets the duration of last checkpoint lock wait in milliseconds.
     *
     * @return Checkpoint lock wait time in milliseconds.
     */
    public long getLastCheckpointLockWaitDuration();

    /**
     * Gets the duration of last checkpoint mark phase in milliseconds.
     *
     * @return Checkpoint mark duration in milliseconds.
     */
    public long getLastCheckpointMarkDuration();

    /**
     * Gets the duration of last checkpoint pages write phase in milliseconds.
     *
     * @return Checkpoint pages write phase in milliseconds.
     */
    public long getLastCheckpointPagesWriteDuration();

    /**
     * Gets the duration of the sync phase of the last checkpoint in milliseconds.
     *
     * @return Checkpoint fsync time in milliseconds.
     */
    public long getLastCheckpointFsyncDuration();

    /**
     * Gets the total number of pages written during the last checkpoint.
     *
     * @return Total number of pages written during the last checkpoint.
     */
    public long getLastCheckpointTotalPagesNumber();

    /**
     * Gets the number of data pages written during the last checkpoint.
     *
     * @return Total number of data pages written during the last checkpoint.
     */
    public long getLastCheckpointDataPagesNumber();

    /**
     * Gets the number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     *
     * @return Total number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     */
    public long getLastCheckpointCopiedOnWritePagesNumber();

    /**
     * Total dirty pages for the next checkpoint.
     *
     * @return  Total dirty pages for the next checkpoint.
     */
    public long getDirtyPages();

    /**
     * The number of read pages from last restart.
     *
     * @return The number of read pages from last restart.
     */
    public long getPagesRead();

    /**
     * The number of written pages from last restart.
     *
     * @return The number of written pages from last restart.
     */
    public long getPagesWritten();

    /**
     * The number of replaced pages from last restart.
     *
     * @return The number of replaced pages from last restart.
     */
    public long getPagesReplaced();

    /**
     * Total offheap size in bytes.
     *
     * @return Total offheap size in bytes.
     */
    public long getOffHeapSize();

    /**
     * Total used offheap size in bytes.
     *
     * @return Total used offheap size in bytes.
     */
    public long getOffheapUsedSize();

    /**
     * Total size of memory allocated in bytes.
     *
     * @return Total size of memory allocated in bytes.
     */
    public long getTotalAllocatedSize();

    /**
     * Gets used checkpoint buffer size in pages.
     *
     * @return Checkpoint buffer size in pages.
     */
    public long getUsedCheckpointBufferPages();

    /**
     * Gets used checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    public long getUsedCheckpointBufferSize();

    /**
     * Checkpoint buffer size in bytes.
     *
     * @return Checkpoint buffer size in bytes.
     */
    public long getCheckpointBufferSize();

    /**
     * Storage space allocated in bytes.
     *
     * @return Storage space allocated in bytes.
     */
    public long getStorageSize();

    /**
     * Storage space allocated adjusted for possible sparsity in bytes.
     *
     * May produce unstable or even incorrect result on some file systems (e.g. XFS).
     * Known to work correctly on Ext4 and Btrfs.
     *
     * @return Storage space allocated adjusted for possible sparsity in bytes
     *         or negative value is not supported.
     */
    public long getSparseStorageSize();
}
