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

/**
 * Interface provides methods to access metrics of memory usage on local instance of Ignite.
 */
public interface MemoryMetrics {
    /**
     * @return Memory policy name.
     */
    public String getName();

    /**
     * @return Returns size (in MBytes) of MemoryPolicy observed by this MemoryMetrics MBean.
     */
    public int getSize();

    /**
     * @return Path of memory-mapped file used to swap PageMemory pages to disk.
     */
    public String getSwapFilePath();

    /**
     * Enables collecting memory metrics.
     */
    public void enableMetrics();

    /**
     * Disables collecting memory metrics.
     */
    public void disableMetrics();

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

    /**
     * Sets interval of time (in seconds) to monitor allocation rate.
     *
     * E.g. after setting rateTimeInterval to 60 seconds subsequent calls to {@link #getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @param rateTimeInterval Time interval used to calculate allocation/eviction rate.
     */
    public void rateTimeInterval(int rateTimeInterval);

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate,
     * 5 by default.
     * Setting it to bigger number allows more precise calculation and smaller drops of allocationRate metric
     * when next subinterval has to be recycled but introduces bigger calculation overhead.
     *
     * @param subInts Number of subintervals.
     */
    public void subIntervals(int subInts);
}
