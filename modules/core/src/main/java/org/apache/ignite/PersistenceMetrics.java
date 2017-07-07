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

import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;

/**
 * Persistence metrics used to obtain statistics on the Ignite Persistent Store.
 *
 * Use {@link Ignite#persistentStoreMetrics()} to obtain this set of metrics.
 */
public interface PersistenceMetrics {
    /**
     * Gets the average number of WAL records per second logged during the last configured time interval.
     * <p>
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configuration property.
     * The number of sub-intervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
     * configuration property.
     */
    public float getWalLoggingRate();

    /**
     * Gets the average number of bytes per second written during the last configured time interval.
     * <p>
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configuration property.
     * The number of sub-intervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
     * configuration property.
     */
    public float getWalWritingRate();

    /**
     * Gets a current number of WAL segments in the WAL archive.
     */
    public int getWalArchiveSegments();

    /**
     * Gets an average WAL fsync duration in microseconds over the last configured time interval.
     * <p>
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configurartion property.
     * The number of subintervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
     * configuration property.
     */
    public float getWalFsyncTimeAverage();

    /**
     * Gets duration of the last checkpoint process in milliseconds.
     *
     * @return Total checkpoint duration in milliseconds.
     */
    public long getLastCheckpointingDuration();

    /**
     * Gets duration of the last checkpoint lock wait time in milliseconds.
     *
     * @return Checkpoint lock wait time in milliseconds.
     */
    public long getLastCheckpointLockWaitDuration();

    /**
     * Gets duration of the last checkpoint mark phase time in milliseconds.
     *
     * @return Checkpoint mark duration in milliseconds.
     */
    public long getLastCheckpointMarkDuration();

    /**
     * Gets duration of the last checkpoint pages write phase time in milliseconds.
     *
     * @return Checkpoint pages write phase in milliseconds.
     */
    public long getLastCheckpointPagesWriteDuration();

    /**
     * Gets duration of the fsync phase of the last checkpoint process in milliseconds.
     *
     * @return Checkpoint fsync time in milliseconds.
     */
    public long getLastCheckpointFsyncDuration();

    /**
     * Gets a total number of pages written during the last checkpoint process.
     *
     * @return Total number of pages written during the last checkpoint.
     */
    public long getLastCheckpointTotalPagesNumber();

    /**
     * Gets a number of data pages written during the last checkpoint process.
     *
     * @return Total number of data pages written during the last checkpoint.
     */
    public long getLastCheckpointDataPagesNumber();

    /**
     * Gets a number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     *
     * @return Total number of pages copied to a temporary checkpoint buffer during the last checkpoint.
     */
    public long getLastCheckpointCopiedOnWritePagesNumber();
}
