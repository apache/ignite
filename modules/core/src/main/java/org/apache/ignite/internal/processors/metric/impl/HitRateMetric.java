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
package org.apache.ignite.internal.processors.metric.impl;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.processors.metric.AbstractMetric;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

/**
 * Accumulates approximate hit rate statistics.
 * Calculates number of hits in last {@code rateTimeInterval} milliseconds.
 * Algorithm is based on circular array of {@code size} hit counters, each is responsible for last corresponding time
 * interval of {@code rateTimeInterval}/{@code size} milliseconds. Resulting number of hits is sum of all counters.
 *
 * <p>Implementation is nonblocking and protected from hits loss.
 * Maximum relative error is 1/{@code size}.
 * 2^55 - 1 hits per interval can be accumulated without numeric overflow.
 */
public class HitRateMetric extends AbstractMetric implements LongMetric {
    /** Default counters array size. */
    public static final int DFLT_SIZE = 10;

    /** Metric instance. */
    private volatile HitRateMetricImpl cntr;

    /**
     * @param name Name.
     * @param desc Description.
     * @param rateTimeInterval Rate time interval in milliseconds.
     * @param size Counters array size.
     */
    public HitRateMetric(String name, @Nullable String desc, long rateTimeInterval, int size) {
        super(name, desc);

        cntr = new HitRateMetricImpl(rateTimeInterval, size);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        HitRateMetricImpl cntr0 = cntr;

        cntr = new HitRateMetricImpl(cntr0.rateTimeInterval, cntr0.size);
    }

    /**
     * Resets metric with the new parametes.
     *
     * @param rateTimeInterval New rate time interval.
     */
    public void reset(long rateTimeInterval) {
        reset(rateTimeInterval, DFLT_SIZE);
    }

    /**
     * Resets metric with the new parameters.
     *
     * @param rateTimeInterval New rate time interval.
     * @param size New counters array size.
     */
    public void reset(long rateTimeInterval, int size) {
        cntr = new HitRateMetricImpl(rateTimeInterval, size);
    }

    /**
     * Adds x to the metric.
     *
     * @param x Value to be added.
     */
    public void add(long x) {
        cntr.add(x);
    }

    /** Adds 1 to the metric. */
    public void increment() {
        add(1);
    }

    /** {@inheritDoc} */
    @Override public long value() {
        return cntr.value();
    }

    /** @return Rate time interval in milliseconds. */
    public long rateTimeInterval() {
        return cntr.rateTimeInterval;
    }

    /**
     * Actual metric.
     *
     * Separated class required to
     */
    private static class HitRateMetricImpl {
        /** Bits that store actual hit count. */
        private static final int TAG_OFFSET = 56;

        /** Useful part mask. */
        private static final long NO_TAG_MASK = ~(-1L << TAG_OFFSET);

        /** Time interval when hits are counted to calculate rate, in milliseconds. */
        private final long rateTimeInterval;

        /** Counters array size. */
        private final int size;

        /** Tagged counters. */
        private final AtomicLongArray taggedCounters;

        /** Last hit times. */
        private final AtomicLongArray lastHitTimes;

        /**
         * @param rateTimeInterval Rate time interval.
         * @param size Number of counters.
         */
        public HitRateMetricImpl(long rateTimeInterval, int size) {
            A.ensure(rateTimeInterval > 0, "rateTimeInterval should be positive");

            A.ensure(size > 1, "Minimum value for size is 2");

            this.rateTimeInterval = rateTimeInterval;

            this.size = size;

            taggedCounters = new AtomicLongArray(size);

            lastHitTimes = new AtomicLongArray(size);
        }

        /**
         * Adds hits to the metric.
         *
         * @param hits Number of hits.
         */
        public void add(long hits) {
            long curTs = U.currentTimeMillis();

            int curPos = position(curTs);

            clearIfObsolete(curTs, curPos);

            lastHitTimes.set(curPos, curTs);

            // Order is important. Hit won't be cleared by concurrent #clearIfObsolete.
            taggedCounters.addAndGet(curPos, hits);
        }

        /**
         * @return Total number of hits in last {@link #rateTimeInterval} milliseconds.
         */
        public long value() {
            long curTs = U.currentTimeMillis();

            long sum = 0;

            for (int i = 0; i < size; i++) {
                clearIfObsolete(curTs, i);

                sum += untag(taggedCounters.get(i));
            }

            return sum;
        }

        /**
         * @param curTs Current timestamp.
         * @param i Index.
         */
        private void clearIfObsolete(long curTs, int i) {
            long cur = taggedCounters.get(i);

            byte curTag = getTag(cur);

            long lastTs = lastHitTimes.get(i);

            if (isObsolete(curTs, lastTs)) {
                if (taggedCounters.compareAndSet(i, cur, taggedLongZero(++curTag))) // ABA problem prevention.
                    lastHitTimes.set(i, curTs);
                // If CAS failed, counter is reset by another thread.
            }
        }

        /**
         * @param curTs Current timestamp.
         * @param lastHitTime Last hit timestamp.
         * @return True, is last hit time was too long ago.
         */
        private boolean isObsolete(long curTs, long lastHitTime) {
            return curTs - lastHitTime > rateTimeInterval * (size - 1) / size;
        }

        /**
         * @param time Timestamp.
         * @return Index of counter for given timestamp.
         */
        private int position(long time) {
            return (int)((time % rateTimeInterval * size) / rateTimeInterval);
        }

        /**
         * @param tag Tag byte.
         * @return 0L with given tag byte.
         */
        private static long taggedLongZero(byte tag) {
            return ((long)tag << TAG_OFFSET);
        }

        /**
         * @param l Tagged long.
         * @return Long without tag byte.
         */
        private static long untag(long l) {
            return l & NO_TAG_MASK;
        }

        /**
         * @param taggedLong Tagged long.
         * @return Tag byte.
         */
        private static byte getTag(long taggedLong) {
            return (byte)(taggedLong >> TAG_OFFSET);
        }
    }
}
