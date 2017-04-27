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
import org.apache.ignite.mxbean.MemoryMetricsMXBean;

/**
 * An interface to collect metrics about page memory usage on Ignite node. Overall page memory architecture
 * is described in {@link MemoryConfiguration} javadoc.
 * <p>
 * As multiple page memories may be configured on a single Ignite node; memory metrics will be collected
 * for each page memory separately.
 * </p>
 * <p>
 * There are two ways to access metrics on local node.
 * <ol>
 *     <li>
 *       Firstly, collection of metrics can be obtained through {@link Ignite#memoryMetrics()} call.<br/>
 *       Please pay attention that this call returns snapshots of memory metrics and not live objects.
 *     </li>
 *     <li>
 *       Secondly, all {@link MemoryMetrics} on local node are exposed through JMX interface. <br/>
 *       See {@link MemoryMetricsMXBean} interface describing information provided about metrics
 *       and page memory configuration.
 *     </li>
 * </ol>
 * </p>
 * <p>
 * Also users must be aware that using memory metrics has some overhead and for performance reasons is turned off
 * by default.
 * For turning them on both {@link MemoryPolicyConfiguration#setMetricsEnabled(boolean)} configuration property
 * or {@link MemoryMetricsMXBean#enableMetrics()} method of JMX bean can be used.
 * </p>
 */
public interface MemoryMetrics {
    /**
     * @return Name of memory region metrics are collected for.
     */
    public String getName();

    /**
     * @return Total number of allocated pages.
     */
    public long getTotalAllocatedPages();

    /**
     * @return Number of allocated pages per second within PageMemory.
     */
    public float getAllocationRate();

    /**
     * @return Number of evicted pages per second within PageMemory.
     */
    public float getEvictionRate();

    /**
     * Large entities bigger than page are split into fragments so each fragment can fit into a page.
     *
     * @return Percentage of pages fully occupied by large entities.
     */
    public float getLargeEntriesPagesPercentage();

    /**
     * @return Free space to overall size ratio across all pages in FreeList.
     */
    public float getPagesFillFactor();
}
