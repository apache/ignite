/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.eviction.sorted;

import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * MBean for sorted eviction policy.
 */
@MXBeanDescription("MBean for sorted cache eviction policy.")
public interface SortedEvictionPolicyMBean {
    /**
     * Gets maximum allowed cache size.
     *
     * @return Maximum allowed cache size.
     */
    @MXBeanDescription("Maximum allowed cache size.")
    public int getMaxSize();

    /**
     * Sets maximum allowed cache size.
     *
     * @param max Maximum allowed cache size.
     */
    @MXBeanDescription("Set maximum allowed cache size.")
    public void setMaxSize(int max);

    /**
     * Gets batch size.
     *
     * @return batch size.
     */
    @MXBeanDescription("Batch size.")
    public int getBatchSize();

    /**
     * Sets batch size.
     *
     * @param batchSize Batch size.
     */
    @MXBeanDescription("Set batch size.")
    public void setBatchSize(int batchSize);

    /**
     * Gets current size.
     *
     * @return Current size.
     */
    @MXBeanDescription("Current sorted key set size.")
    public int getCurrentSize();

    /**
     * Gets maximum allowed cache size in bytes.
     *
     * @return maximum allowed cache size in bytes.
     */
    @MXBeanDescription("Maximum allowed cache size in bytes.")
    public long getMaxMemorySize();

    /**
     * Sets maximum allowed cache size in bytes.
     */
    @MXBeanDescription("Set maximum allowed cache size in bytes.")
    public void setMaxMemorySize(long maxMemSize);

    /**
     * Gets current sorted entries queue size in bytes.
     *
     * @return current sorted entries queue size in bytes.
     */
    @MXBeanDescription("Current sorted entries set size in bytes.")
    public long getCurrentMemorySize();
}