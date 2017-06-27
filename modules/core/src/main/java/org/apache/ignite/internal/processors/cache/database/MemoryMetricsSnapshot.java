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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.MemoryMetrics;

/**
 *
 */
public class MemoryMetricsSnapshot implements MemoryMetrics {
    /** */
    private String name;

    /** */
    private long totalAllocatedPages;

    /** */
    private float allocationRate;

    /** */
    private float evictionRate;

    /** */
    private float largeEntriesPagesPercentage;

    /** */
    private float pagesFillFactor;

    /**
     * @param metrics Metrics instance to take a copy.
     */
    public MemoryMetricsSnapshot(MemoryMetrics metrics) {
        name = metrics.getName();
        totalAllocatedPages = metrics.getTotalAllocatedPages();
        allocationRate = metrics.getAllocationRate();
        evictionRate = metrics.getEvictionRate();
        largeEntriesPagesPercentage = metrics.getLargeEntriesPagesPercentage();
        pagesFillFactor = metrics.getPagesFillFactor();
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
}
