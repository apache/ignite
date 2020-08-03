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

/**
 * Persistence metrics used to obtain statistics on persistence.
 *
 * @deprecated Use {@link DataStorageMetrics} instead.
 */
@Deprecated
public interface PersistenceMetrics {
    /**
     * Gets the average number of WAL records per second written during the last time interval.
     * <p>
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configurartion property.
     * The number of subintervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
     * configuration property.
     */
    public float getWalLoggingRate();

    /**
     * Gets the average number of bytes per second written during the last time interval.
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configurartion property.
     * The number of subintervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
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
     * The length of time interval is configured via {@link PersistentStoreConfiguration#setRateTimeInterval(long)}
     * configurartion property.
     * The number of subintervals is configured via {@link PersistentStoreConfiguration#setSubIntervals(int)}
     * configuration property.
     */
    public float getWalFsyncTimeAverage();

    /**
     * Gets the duration of the last checkpoint in milliseconds.
     *
     * @return Total checkpoint duration in milliseconds.
     */
    public long getLastCheckpointingDuration();

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
}
