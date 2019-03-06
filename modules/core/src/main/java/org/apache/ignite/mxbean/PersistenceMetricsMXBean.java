/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.mxbean;

import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.configuration.PersistentStoreConfiguration;

/**
 * An MX bean allowing to monitor and tune persistence metrics.
 * @deprecated Part of old API. Metrics are accessible through {@link DataStorageMetricsMXBean}.
 */
@Deprecated
public interface PersistenceMetricsMXBean extends PersistenceMetrics {
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
    @MXBeanDescription("Duration of the last checkpoint in milliseconds.")
    @Override long getLastCheckpointingDuration();

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
     * {@link PersistentStoreConfiguration#setRateTimeInterval(long)} configuration property.
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
     * rate-based metrics. Identical to setting {@link PersistentStoreConfiguration#setSubIntervals(int)} configuration
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
