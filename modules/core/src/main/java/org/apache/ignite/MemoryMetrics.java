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

import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;

/**
 * This interface provides page memory related metrics of a specific Apache Ignite node. The overall page memory
 * architecture is covered in {@link MemoryConfiguration}.
 * <p>
 * Since there are can be several memory regions configured with {@link MemoryPolicyConfiguration} on an individual
 * Apache Ignite node, the metrics for every region will be collected and obtained separately.
 * <p>
 * There are two ways to get the metrics of an Apache Ignite node.
 * <ol>
 *     <li>
 *       First, a collection of the metrics can be obtained through {@link Ignite#dataRegionMetrics()} ()} method. Note that
 *       the method returns memory metrics snapshots rather than just in time memory state.
 *     </li>
 *     <li>
 *       Second, all {@link MemoryMetrics} of a local Apache Ignite node are visible through JMX interface. Refer to
 *       {@link DataRegionMetricsMXBean} for more details.
 *     </li>
 * </ol>
 * </p>
 * <p>
 * Memory metrics collection is not a free operation and might affect performance of an application. This is the reason
 * why the metrics are turned off by default. To enable the collection you can use both
 * {@link MemoryPolicyConfiguration#setMetricsEnabled(boolean)} configuration property or
 * {@link DataRegionMetricsMXBean#enableMetrics()} method of a respective JMX bean.
 *
 * @deprecated Use {@link DataRegionMetrics} instead.
 */
@Deprecated
public interface MemoryMetrics {
    /**
     * A name of a memory region the metrics are collected for.
     *
     * @return Name of the memory region.
     */
    public String getName();

    /**
     * Gets a total number of allocated pages related to the memory policy. When persistence is disabled, this
     * metric shows the total number of pages in memory. When persistence is enabled, this metric shows the
     * total number of pages in memory and on disk.
     *
     * @return Total number of allocated pages.
     */
    public long getTotalAllocatedPages();

    /**
     * Gets pages allocation rate of a memory region.
     *
     * @return Number of allocated pages per second.
     */
    public float getAllocationRate();

    /**
     * Gets eviction rate of a given memory region.
     *
     * @return Number of evicted pages per second.
     */
    public float getEvictionRate();

    /**
     * Gets percentage of pages that are fully occupied by large entries that go beyond page size. The large entities
     * are split into fragments in a way so that each fragment can fit into a single page.
     *
     * @return Percentage of pages fully occupied by large entities.
     */
    public float getLargeEntriesPagesPercentage();

    /**
     * Gets the percentage of space that is still free and can be filled in.
     *
     * @return The percentage of space that is still free and can be filled in.
     */
    public float getPagesFillFactor();

    /**
     * Gets the number of dirty pages (pages which contents is different from the current persistent storage state).
     * This metric is enabled only for Ignite nodes with enabled persistence.
     *
     * @return Current number of dirty pages.
     */
    public long getDirtyPages();

    /**
     * Gets rate (pages per second) at which pages get replaced with other pages from persistent storage.
     * The rate effectively represents the rate at which pages get 'evicted' in favor of newly needed pages.
     * This metric is enabled only for Ignite nodes with enabled persistence.
     *
     * @return Pages per second replace rate.
     */
    public float getPagesReplaceRate();

    /**
     * Gets total number of pages currently loaded to the RAM. When persistence is disabled, this metric is equal
     * to {@link #getTotalAllocatedPages()}.
     *
     * @return Total number of pages loaded to RAM.
     */
    public long getPhysicalMemoryPages();
}
