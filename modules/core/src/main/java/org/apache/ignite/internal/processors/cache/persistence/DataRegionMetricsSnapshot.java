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
    private final String name;

    /** */
    private final long totalAllocatedPages;

    /** */
    private final long totalAllocatedSize;

    /** */
    private final long totalUsedPages;

    /** */
    private final long totalUsedSize;

    /** */
    private final float allocationRate;

    /** */
    private final float evictionRate;

    /** */
    private final float largeEntriesPagesPercentage;

    /** */
    private final float pagesFillFactor;

    /** */
    private final long dirtyPages;

    /** */
    private final float pageReplaceRate;

    /** */
    private final float pageReplaceAge;

    /** */
    private final long physicalMemoryPages;

    /** */
    private final long physicalMemorySize;

    /** */
    private final long usedCheckpointBufferPages;

    /** */
    private final long usedCheckpointBufferSize;

    /** */
    private final long checkpointBufferSize;

    /** */
    private final int pageSize;

    /** */
    private final long readPages;

    /** */
    private final long writtenPages;

    /** */
    private final long replacedPage;

    /** */
    private final long offHeapSize;

    /** */
    private final long offHeapUsedSize;

    /**
     * @param metrics Metrics instance to take a copy.
     */
    public DataRegionMetricsSnapshot(DataRegionMetrics metrics) {
        name = metrics.getName();
        totalAllocatedPages = metrics.getTotalAllocatedPages();
        totalAllocatedSize = metrics.getTotalAllocatedSize();
        totalUsedPages = metrics.getTotalUsedPages();
        totalUsedSize = metrics.getTotalUsedSize();
        allocationRate = metrics.getAllocationRate();
        evictionRate = metrics.getEvictionRate();
        largeEntriesPagesPercentage = metrics.getLargeEntriesPagesPercentage();
        pagesFillFactor = metrics.getPagesFillFactor();
        dirtyPages = metrics.getDirtyPages();
        pageReplaceRate = metrics.getPagesReplaceRate();
        pageReplaceAge = metrics.getPagesReplaceAge();
        physicalMemoryPages = metrics.getPhysicalMemoryPages();
        physicalMemorySize = metrics.getPhysicalMemorySize();
        usedCheckpointBufferPages = metrics.getUsedCheckpointBufferPages();
        usedCheckpointBufferSize = metrics.getUsedCheckpointBufferSize();
        checkpointBufferSize = metrics.getCheckpointBufferSize();
        pageSize = metrics.getPageSize();
        readPages = metrics.getPagesRead();
        writtenPages = metrics.getPagesWritten();
        replacedPage = metrics.getPagesReplaced();
        offHeapSize = metrics.getOffHeapSize();
        offHeapUsedSize = metrics.getOffheapUsedSize();
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
    @Override public long getTotalAllocatedSize() {
        return totalAllocatedSize;
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return totalUsedPages;
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedSize() {
        return totalUsedSize;
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

    /** {@inheritDoc} */
    @Override public long getPhysicalMemorySize() {
        return physicalMemorySize;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        return usedCheckpointBufferPages;
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return usedCheckpointBufferSize;
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        return checkpointBufferSize;
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        return readPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        return writtenPages;
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        return replacedPage;
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return offHeapSize;
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        return offHeapUsedSize;
    }
}
