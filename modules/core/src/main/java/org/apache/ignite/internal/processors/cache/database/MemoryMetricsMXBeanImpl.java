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
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.mxbean.MemoryMetricsMXBean;

/**
 * MBean to expose {@link MemoryMetrics} through JMX interface.
 */
class MemoryMetricsMXBeanImpl implements MemoryMetricsMXBean {
    /** */
    private final MemoryMetricsImpl memMetrics;

    /** */
    private final MemoryPolicyConfiguration memPlcCfg;

    /**
     * @param memMetrics MemoryMetrics instance to expose through JMX interface.
     * @param memPlcCfg configuration of memory policy this MX Bean is created for.
     */
    MemoryMetricsMXBeanImpl(MemoryMetricsImpl memMetrics,
        MemoryPolicyConfiguration memPlcCfg
    ) {
        this.memMetrics = memMetrics;
        this.memPlcCfg = memPlcCfg;
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
    @Override public void rateTimeInterval(int rateTimeInterval) {
        memMetrics.rateTimeInterval(rateTimeInterval);
    }

    /** {@inheritDoc} */
    @Override public void subIntervals(int subInts) {
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
        return (int) (memPlcCfg.getInitialSize() / (1024 * 1024));
    }

    /** {@inheritDoc} */
    @Override public int getMaxSize() {
        return (int) (memPlcCfg.getMaxSize() / (1024 * 1024));
    }

    /** {@inheritDoc} */
    @Override public String getSwapFilePath() {
        return memPlcCfg.getSwapFilePath();
    }
}
