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
package org.apache.ignite.internal.processors.cache.ratemetrics;

import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Accumulates approximate hit rate statistics.
 * Calculates number of hits in last {@link #rateTimeInterval} milliseconds.
 * Algorithm is based on circular array of {@link #size} hit counters, each is responsible for last corresponding time
 * interval of {@link #rateTimeInterval}/{@link #size} milliseconds. Resulting number of hits is sum of all counters.
 *
 * <p>Implementation is nonblocking and protected from hits loss.
 * Maximum relative error is 1/{@link #size}.
 * 2^55 - 1 hits per interval can be accumulated without numeric overflow.
 */
public class HitRateMetrics {
    /** Bits that store actual hit count. */
    private static final int TAG_OFFSET = 56;

    /** Useful part mask. */
    private static final long NO_TAG_MASK = ~(-1L << TAG_OFFSET);

    /** Time interval when hits are counted to calculate rate, in milliseconds. */
    private final int rateTimeInterval;

    /** Counters array size. */
    private final int size;

    /** Tagged counters. */
    private volatile AtomicLongArray taggedCounters;

    /** Last hit times. */
    private volatile AtomicLongArray lastHitTimes;

    /**
     * @param rateTimeInterval Rate time interval.
     * @param size Number of counters.
     */
    public HitRateMetrics(int rateTimeInterval, int size) {
        A.ensure(rateTimeInterval > 0, "should be positive");

        A.ensure(size > 1, "minimum is 2");

        this.rateTimeInterval = rateTimeInterval;

        this.size = size;

        taggedCounters = new AtomicLongArray(size);

        lastHitTimes = new AtomicLongArray(size);
    }

    /**
     * Mark one hit.
     */
    public void onHit() {
        onHits(1);
    }

    /**
     * Mark multiple hits.
     *
     * @param hits Number of hits.
     */
    public void onHits(long hits) {
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
    public long getRate() {
        long curTs = U.currentTimeMillis();

        long sum = 0;

        for (int i = 0; i < size; i++) {
            clearIfObsolete(curTs, i);

            sum += untag(taggedCounters.get(i));
        }

        return sum;
    }

    /**
     * Clear metrics.
     */
    public void clear() {
        taggedCounters = new AtomicLongArray(size);

        lastHitTimes = new AtomicLongArray(size);
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
        return (int)(time % rateTimeInterval * size) / rateTimeInterval;
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
