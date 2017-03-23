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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jsr166.LongAdder8;

/**
 *
 */
public class MemoryMetricsImpl implements MemoryMetrics {
    /** */
    private final String name;

    /** */
    private final LongAdder8 totalAllocatedPages = new LongAdder8();

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final LongAdder8 largeEntriesPages = new LongAdder8();

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private final LongAdder8[] cntrs = new LongAdder8[5];

    /** */
    private final AtomicInteger cntIndex = new AtomicInteger(0);

    /** */
    private final AtomicLong lastUpdatedTime = new AtomicLong(0);

    /** 60 seconds by default */
    private volatile int rateTimeInterval = 60;

    /**
     * @param name Name.
     */
    public MemoryMetricsImpl(String name) {
        this.name = name;

        for (int i = 0; i < 5; i++)
            cntrs[i] = new LongAdder8();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return metricsEnabled ? totalAllocatedPages.longValue() : 0;
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (metricsEnabled)
            return totalAllocatedPages.longValue() != 0 ?
                    (float) largeEntriesPages.doubleValue() / totalAllocatedPages.longValue()
                    : 0;
        else
            return 0;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        return 0;
    }

    /**
     * Increments totalAllocatedPages counter.
     */
    public void incrementTotalAllocatedPages() {
        if (metricsEnabled) {
            totalAllocatedPages.increment();

            updateAllocationRateMetrics();
        }
    }

    private void updateAllocationRateMetrics() {
        IgniteUtils.currentTimeMillis();
    }

    /**
     *
     */
    public void incrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.increment();
    }

    /**
     *
     */
    public void decrementLargeEntriesPages() {
        if (metricsEnabled)
            largeEntriesPages.decrement();
    }

    @Override public void enableMetrics() {
        metricsEnabled = true;
    }

    @Override public void disableMetrics() {
        metricsEnabled = false;
    }

    /**
     * @param rateTimeInterval Time interval used to calculate allocation/eviction rate.
     */
    public void rateTimeInterval(int rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;
    }
}
