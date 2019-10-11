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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;

/**
 * MBean to expose {@link DataRegionMetrics} through JMX interface.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
class DataRegionMetricsMXBeanImpl implements DataRegionMetricsMXBean {
    /** */
    private final DataRegionMetricsImpl memMetrics;

    /** */
    private final DataRegionConfiguration dataRegCfg;

    /**
     * @param memMetrics DataRegionMetrics instance to expose through JMX interface.
     * @param dataRegCfg Configuration of data region this MX Bean is created for.
     */
    DataRegionMetricsMXBeanImpl(DataRegionMetricsImpl memMetrics,
        DataRegionConfiguration dataRegCfg
    ) {
        this.memMetrics = memMetrics;
        this.dataRegCfg = dataRegCfg;
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        return memMetrics.getAllocationRate();
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return memMetrics.getEvictionRate();
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        return memMetrics.getLargeEntriesPagesPercentage();
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        return memMetrics.getPagesFillFactor();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return memMetrics.getTotalAllocatedPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalUsedPages() {
        return memMetrics.getTotalUsedPages();
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedSize() {
        return memMetrics.getTotalAllocatedSize();
    }

    /** {@inheritDoc} */
    @Override public long getDirtyPages() {
        return memMetrics.getDirtyPages();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceRate() {
        return memMetrics.getPagesReplaceRate();
    }

    /** {@inheritDoc} */
    @Override public float getPagesReplaceAge() {
        return memMetrics.getPagesReplaceAge();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemoryPages() {
        return memMetrics.getPhysicalMemoryPages();
    }

    /** {@inheritDoc} */
    @Override public long getPhysicalMemorySize() {
        return memMetrics.getPhysicalMemorySize();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferPages() {
        return memMetrics.getUsedCheckpointBufferPages();
    }

    /** {@inheritDoc} */
    @Override public long getUsedCheckpointBufferSize() {
        return memMetrics.getUsedCheckpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointBufferSize() {
        return memMetrics.getCheckpointBufferSize();
    }

    /** {@inheritDoc} */
    @Override public int getPageSize() {
        return memMetrics.getPageSize();
    }

    /** {@inheritDoc} */
    @Override public void rateTimeInterval(long rateTimeInterval) {
        if (rateTimeInterval < 1000)
            throw new IllegalArgumentException("rateTimeInterval property must be positive " +
                "and greater than 1_000 milliseconds (one second)");

        memMetrics.rateTimeInterval(rateTimeInterval);
    }

    /** {@inheritDoc} */
    @Override public void subIntervals(int subInts) {
        if (subInts <= 1)
            throw new IllegalArgumentException("subIntervals property must be positive " +
                "and greater than one");

        memMetrics.subIntervals(subInts);
    }

    /** {@inheritDoc} */
    @Override public void enableMetrics() {
        memMetrics.enableMetrics();
    }

    /** {@inheritDoc} */
    @Override public void disableMetrics() {
        memMetrics.disableMetrics();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return memMetrics.getName();
    }

    /** {@inheritDoc} */
    @Override public int getInitialSize() {
        return (int) (dataRegCfg.getInitialSize() / (1024 * 1024));
    }

    /** {@inheritDoc} */
    @Override public int getMaxSize() {
        return (int) (dataRegCfg.getMaxSize() / (1024 * 1024));
    }

    /** {@inheritDoc} */
    @Override public String getSwapPath() {
        return dataRegCfg.getSwapPath();
    }

    /** {@inheritDoc} */
    @Override public long getPagesRead() {
        return memMetrics.getPagesRead();
    }

    /** {@inheritDoc} */
    @Override public long getPagesWritten() {
        return memMetrics.getPagesWritten();
    }

    /** {@inheritDoc} */
    @Override public long getPagesReplaced() {
        return memMetrics.getPagesReplaced();
    }

    /** {@inheritDoc} */
    @Override public long getOffHeapSize() {
        return memMetrics.getOffHeapSize();
    }

    /** {@inheritDoc} */
    @Override public long getOffheapUsedSize() {
        return memMetrics.getOffheapUsedSize();
    }
}
