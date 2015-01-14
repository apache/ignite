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

package org.gridgain.grid.cache.eviction.ggfs;

import org.apache.ignite.mbean.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * MBean for {@code GGFS per-block LRU} eviction policy.
 */
@IgniteMBeanDescription("MBean for GGFS per-block LRU cache eviction policy.")
public interface GridCacheGgfsPerBlockLruEvictionPolicyMBean {
    /**
     * Gets maximum allowed size of all blocks in bytes.
     *
     * @return Maximum allowed size of all blocks in bytes.
     */
    @IgniteMBeanDescription("Maximum allowed size of all blocks in bytes.")
    public long getMaxSize();

    /**
     * Sets maximum allowed size of data in all blocks in bytes.
     *
     * @param maxSize Maximum allowed size of data in all blocks in bytes.
     */
    @IgniteMBeanDescription("Sets aximum allowed size of data in all blocks in bytes.")
    public void setMaxSize(long maxSize);

    /**
     * Gets maximum allowed amount of blocks.
     *
     * @return Maximum allowed amount of blocks.
     */
    @IgniteMBeanDescription("Maximum allowed amount of blocks.")
    public int getMaxBlocks();

    /**
     * Sets maximum allowed amount of blocks.
     *
     * @param maxBlocks Maximum allowed amount of blocks.
     */
    @IgniteMBeanDescription("Sets maximum allowed amount of blocks.")
    public void setMaxBlocks(int maxBlocks);

    /**
     * Gets collection of regex for paths whose blocks must not be evicted.
     *
     * @return Collection of regex for paths whose blocks must not be evicted.
     */
    @IgniteMBeanDescription("Collection of regex for paths whose blocks must not be evicted.")
    @Nullable public Collection<String> getExcludePaths();

    /**
     * Sets collection of regex for paths whose blocks must not be evicted.
     *
     * @param excludePaths Collection of regex for paths whose blocks must not be evicted.
     */
    @IgniteMBeanDescription("Sets collection of regex for paths whose blocks must not be evicted.")
    public void setExcludePaths(@Nullable Collection<String> excludePaths);

    /**
     * Gets current size of data in all blocks.
     *
     * @return Current size of data in all blocks.
     */
    @IgniteMBeanDescription("Current size of data in all blocks.")
    public long getCurrentSize();

    /**
     * Gets current amount of blocks.
     *
     * @return Current amount of blocks.
     */
    @IgniteMBeanDescription("Current amount of blocks.")
    public int getCurrentBlocks();
}
