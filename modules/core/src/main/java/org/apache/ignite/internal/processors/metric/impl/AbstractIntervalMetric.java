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
 * Accumulates approximate values statistics.
 * Calculates accumulated value in last {@code timeInterval} milliseconds.
 * Algorithm is based on circular array of {@code size} values, each is responsible for last corresponding time
 * subinterval of {@code timeInterval}/{@code size} milliseconds. Resulting value is accumulated for all subintervals.
 *
 * Implementation is nonblocking and protected from values loss.
 * Maximum relative error is 1/{@code size}.
 * Maximum value per interval is {@code 2^55 - 1}.
 */
public abstract class AbstractIntervalMetric extends AbstractMetric implements LongMetric {
    /** Default values array size (number of buckets). */
    public static final int DFLT_SIZE = 10;

    /** Metric instance. */
    protected volatile AbstractIntervalMetricImpl cntr;

    /**
     * @param name Name.
     * @param desc Description.
     * @param timeInterval Time interval in milliseconds.
     * @param size Values array size.
     */
    protected AbstractIntervalMetric(String name, @Nullable String desc, long timeInterval, int size) {
        super(name, desc);

        reset(timeInterval, size);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        AbstractIntervalMetricImpl cntr0 = cntr;

        reset(cntr0.timeInterval, cntr0.size);
    }

    /**
     * Resets metric with the new parametes.
     *
     * @param timeInterval New rate time interval.
     */
    public void reset(long timeInterval) {
        reset(timeInterval, DFLT_SIZE);
    }

    /**
     * Resets metric with the new parameters.
     *
     * @param timeInterval New time interval.
     * @param size New values array size.
     */
    public void reset(long timeInterval, int size) {
        cntr = createImpl(timeInterval, size);
    }

    /** */
    protected abstract AbstractIntervalMetricImpl createImpl(long timeInterval, int size);

    /** {@inheritDoc} */
    @Override public long value() {
        return cntr.value();
    }

    /** @return Time interval in milliseconds. */
    public long timeInterval() {
        return cntr.timeInterval;
    }

    /**
     * Actual metric.
     */
    protected abstract static class AbstractIntervalMetricImpl {
        /** Bits that store actual hit count. */
        private static final int TAG_OFFSET = 56;

        /** Tag mask. */
        protected static final long TAG_MASK = -1L << TAG_OFFSET;

        /** Useful part mask. */
        protected static final long NO_TAG_MASK = ~TAG_MASK;

        /** Time interval when values are counted to calculate accumulated value, in milliseconds. */
        private final long timeInterval;

        /** Counters array size. */
        private final int size;

        /** Last value times. */
        private final AtomicLongArray lastValTimes;

        /** Tagged values. */
        protected final AtomicLongArray taggedVals;

        /**
         * @param timeInterval Time interval.
         * @param size Number of buckets.
         */
        protected AbstractIntervalMetricImpl(long timeInterval, int size) {
            A.ensure(timeInterval > 0, "timeInterval should be positive");

            A.ensure(size > 1, "Minimum value for size is 2");

            this.timeInterval = timeInterval;

            this.size = size;

            taggedVals = new AtomicLongArray(size);

            lastValTimes = new AtomicLongArray(size);
        }

        /**
         * Adds val to the metric.
         *
         * @param val Value.
         */
        public void update(long val) {
            long curTs = U.currentTimeMillis();

            int curPos = position(curTs);

            clearIfObsolete(curTs, curPos);

            lastValTimes.set(curPos, curTs);

            // Order is important. Value won't be cleared by concurrent #clearIfObsolete.
            accumulateBucket(curPos, val);
        }

        /**
         * @return Accumulated value in last {@link #timeInterval} milliseconds.
         */
        public long value() {
            long curTs = U.currentTimeMillis();

            long res = 0;

            for (int i = 0; i < size; i++) {
                clearIfObsolete(curTs, i);

                res = accumulate(res, untag(taggedVals.get(i)));
            }

            return res;
        }

        /**
         * Folds value into a result.
         *
         * @return Accumulated value.
         */
        protected abstract long accumulate(long res, long val);

        /**
         * Folds value into a bucket.
         */
        protected abstract void accumulateBucket(int bucket, long val);

        /**
         * @param curTs Current timestamp.
         * @param i Index.
         */
        private void clearIfObsolete(long curTs, int i) {
            long cur = taggedVals.get(i);

            byte curTag = getTag(cur);

            long lastTs = lastValTimes.get(i);

            if (isObsolete(curTs, lastTs)) {
                if (taggedVals.compareAndSet(i, cur, taggedLongZero(++curTag))) // ABA problem prevention.
                    lastValTimes.set(i, curTs);
                // If CAS failed, counter is reset by another thread.
            }
        }

        /**
         * @param curTs Current timestamp.
         * @param lastValTime Last value timestamp.
         * @return True, is last value time was too long ago.
         */
        private boolean isObsolete(long curTs, long lastValTime) {
            return curTs - lastValTime > timeInterval * (size - 1) / size;
        }

        /**
         * @param time Timestamp.
         * @return Index of bucket for given timestamp.
         */
        private int position(long time) {
            return (int)((time % timeInterval * size) / timeInterval);
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
