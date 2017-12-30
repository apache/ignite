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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.DataRegionMetrics;

/**
 *
 */
public class DataRegionMetricsSnapshot implements DataRegionMetrics {
    /** */
    private String name;

    /** */
    private long totalAllocatedPages;

    /** */
    private long indexesAllocatedPages;

    /** */
    private float allocationRate;

    /** */
    private float evictionRate;

    /** */
    private float largeEntriesPagesPercentage;

    /** */
    private float pagesFillFactor;

    /** */
    private long dirtyPages;

    /** */
    private float pageReplaceRate;

    /** */
    private float pageReplaceAge;

    /** */
    private long physicalMemoryPages;

    /**
     * @param metrics Metrics instance to take a copy.
     */
    public DataRegionMetricsSnapshot(DataRegionMetrics metrics) {
        name = metrics.getName();
        totalAllocatedPages = metrics.getTotalAllocatedPages();
        indexesAllocatedPages = metrics.getIndexesAllocatedPages();
        allocationRate = metrics.getAllocationRate();
        evictionRate = metrics.getEvictionRate();
        largeEntriesPagesPercentage = metrics.getLargeEntriesPagesPercentage();
        pagesFillFactor = metrics.getPagesFillFactor();
        dirtyPages = metrics.getDirtyPages();
        pageReplaceRate = metrics.getPagesReplaceRate();
        pageReplaceAge = metrics.getPagesReplaceAge();
        physicalMemoryPages = metrics.getPhysicalMemoryPages();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return totalAllocatedPages;
    }

    /** {@inheritDoc} */
    @Override public long getIndexesAllocatedPages() {
        return indexesAllocatedPages;
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        return allocationRate;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return evictionRate;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        return largeEntriesPagesPercentage;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        return pagesFillFactor;
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return dirtyPages;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        return pageReplaceRate;
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        return pageReplaceAge;
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        return physicalMemoryPages;
    }
}
