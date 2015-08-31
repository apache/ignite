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

package org.apache.ignite.cache.eviction.lru;

import org.apache.ignite.mxbean.MXBeanDescription;

/**
 * MBean for {@code LRU} eviction policy.
 */
@MXBeanDescription("MBean for LRU cache eviction policy.")
public interface LruEvictionPolicyMBean {
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
    @MXBeanDescription("Sets maximum allowed cache size.")
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
     * Gets current queue size.
     *
     * @return Current queue size.
     */
    @MXBeanDescription("Current queue size.")
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
     * Gets current queue size in bytes.
     *
     * @return current queue size in bytes.
     */
    @MXBeanDescription("Current queue size in  bytes.")
    public long getCurrentMemorySize();
}