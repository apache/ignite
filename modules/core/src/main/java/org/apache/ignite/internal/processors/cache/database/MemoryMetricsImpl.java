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
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.LongAdder8;

/**
 *
 */
public class MemoryMetricsImpl implements MemoryMetrics {
    /** */
    private FreeListImpl freeList;

    /** */
    private final LongAdder8 totalAllocatedPages = new LongAdder8();

    /**
     * Counter for number of pages occupied by large entries (one entry is larger than one page).
     */
    private final LongAdder8 largeEntriesPages = new LongAdder8();

    /** */
    private volatile boolean metricsEnabled;

    /** */
    private volatile int subInts = 5;

    /** */
    private volatile LongAdder8[] allocRateCounters = new LongAdder8[subInts];

    /** */
    private final AtomicInteger counterIdx = new AtomicInteger(0);

    /** */
    private final AtomicLong lastUpdTime = new AtomicLong(0);

    /** */
    private final MemoryPolicyConfiguration memPlcCfg;

    /** Time interval (in seconds) when allocations/evictions are counted to calculate rate.
     * Default value is 60 seconds. */
    private volatile int rateTimeInterval = 60;

    /**
     * @param memPlcCfg MemoryPolicyConfiguration.
     */
    public MemoryMetricsImpl(MemoryPolicyConfiguration memPlcCfg) {
        this.memPlcCfg = memPlcCfg;

        metricsEnabled = memPlcCfg.isMetricsEnabled();

        for (int i = 0; i < subInts; i++)
            allocRateCounters[i] = new LongAdder8();
    }

    /** {@inheritDoc} */
    @Override public String getName() {
        return U.maskName(memPlcCfg.getName());
    }

    /** {@inheritDoc} */
    @Override public long getTotalAllocatedPages() {
        return metricsEnabled ? totalAllocatedPages.longValue() : 0;
    }

    /** {@inheritDoc} */
    @Override public float getAllocationRate() {
        if (!metricsEnabled)
            return 0;

        float res = 0;

        for (int i = 0; i < subInts; i++)
            res += allocRateCounters[i].floatValue();

        return res / rateTimeInterval;
    }

    /** {@inheritDoc} */
    @Override public float getEvictionRate() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public float getLargeEntriesPagesPercentage() {
        if (!metricsEnabled)
            return 0;

        return totalAllocatedPages.longValue() != 0 ?
                (float) largeEntriesPages.doubleValue() / totalAllocatedPages.longValue()
                : 0;
    }

    /** {@inheritDoc} */
    @Override public float getPagesFillFactor() {
        if (!metricsEnabled || freeList == null)
            return 0;

        return freeList.fillFactor();
    }

    /**
     * Increments totalAllocatedPages counter.
     */
    public void incrementTotalAllocatedPages() {
        if (metricsEnabled) {
            totalAllocatedPages.increment();

            try {
                updateAllocationRateMetrics();
            }
            catch (ArrayIndexOutOfBoundsException | NullPointerException ignored) {
                // No-op.
            }
        }
    }

    /**
     *
     */
    private void updateAllocationRateMetrics() {
        if (subInts != allocRateCounters.length)
            return;

        long lastUpdT = lastUpdTime.get();
        long currT = IgniteUtils.currentTimeMillis();

        int currIdx = counterIdx.get();

        long deltaT = currT - lastUpdT;

        int subInts = this.subInts;

        LongAdder8[] rateCntrs = allocRateCounters;

        for (int i = 1; i <= subInts; i++) {
            if (deltaT < subInt(i)) {
                if (i > 1) {
                    if (!lastUpdTime.compareAndSet(lastUpdT, currT)) {
                        rateCntrs[counterIdx.get()].increment();

                        break;
                    }
                }

                if (rotateIndex(currIdx, i - 1)) {
                    currIdx = counterIdx.get();

                    resetCounters(currIdx, i - 1);

                    rateCntrs[currIdx].increment();

                    break;
                }
                else {
                    rateCntrs[counterIdx.get()].increment();

                    break;
                }
            }
            else if (i == subInts && lastUpdTime.compareAndSet(lastUpdT, currT))
                resetAll();

            if (currIdx != counterIdx.get()) {
                rateCntrs[counterIdx.get()].increment();

                break;
            }
        }
    }

    /**
     * @param intervalNum Interval number.
     */
    private int subInt(int intervalNum) {
        return (rateTimeInterval * 1000 * intervalNum) / subInts;
    }

    /**
     * @param idx Index.
     * @param rotateStep Rotate step.
     */
    private boolean rotateIndex(int idx, int rotateStep) {
        if (rotateStep == 0)
            return true;

        int subInts = this.subInts;

        assert rotateStep < subInts;

        int nextIdx = (idx + rotateStep) % subInts;

        return counterIdx.compareAndSet(idx, nextIdx);
    }

    /**
     *
     */
    private void resetAll() {
        LongAdder8[] cntrs = allocRateCounters;

        for (LongAdder8 cntr : cntrs)
            cntr.reset();
    }

    /**
     * @param currIdx Current index.
     * @param resettingCntrs Resetting allocRateCounters.
     */
    private void resetCounters(int currIdx, int resettingCntrs) {
        if (resettingCntrs == 0)
            return;

        for (int j = 0; j < resettingCntrs; j++) {
            int cleanIdx = currIdx - j >= 0 ? currIdx - j : currIdx - j + subInts;

            allocRateCounters[cleanIdx].reset();
        }
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

    /**
     * Enable metrics.
     */
    public void enableMetrics() {
        metricsEnabled = true;
    }

    /**
     * Disable metrics.
     */
    public void disableMetrics() {
        metricsEnabled = false;
    }

    /**
     * @param rateTimeInterval Time interval used to calculate allocation/eviction rate.
     */
    public void rateTimeInterval(int rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;
    }

    /**
     * Sets number of subintervals the whole rateTimeInterval will be split into to calculate allocation rate.
     *
     * @param subInts Number of subintervals.
     */
    public void subIntervals(int subInts) {
        assert subInts > 0;

        if (this.subInts == subInts)
            return;

        int rateIntervalMs = rateTimeInterval * 1000;

        if (rateIntervalMs / subInts < 10)
            subInts = rateIntervalMs / 10;

        LongAdder8[] newCounters = new LongAdder8[subInts];

        for (int i = 0; i < subInts; i++)
            newCounters[i] = new LongAdder8();

        this.subInts = subInts;
        allocRateCounters = newCounters;
    }

    /**
     * @param freeList Free list.
     */
    void freeList(FreeListImpl freeList) {
        this.freeList = freeList;
    }
}
