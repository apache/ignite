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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Histogram to show count of pages last accessed in each time interval.
 */
public class PageTimestampHistogram {
    /** Default buckets interval in milliseconds. */
    public static final long DFLT_BUCKETS_INTERVAL = 60L * 60 * 1000; // 60 mins.

    /** Default buckets count. */
    public static final int DFLT_BUCKETS_CNT = 24;

    /** Starting point for bucket index calculation. */
    private final long startTs;

    /** Buckets interval in milliseconds. */
    private final long bucketsInterval;

    /** Buckets count. */
    private final int bucketsCnt;

    /** Upper bound for values stored in buckets array (excluding). */
    private volatile long upperBoundTs;

    /** Lower bound for values stored in buckets array (including). */
    private volatile long lowerBoundTs;

    /** Out of bounds bucket. Contain count of pages which have last access time beyond lowerBoundTs. */
    private final AtomicLong outOfBoundsBucket = new AtomicLong();

    /** Buckets holder. */
    private final AtomicLongArray buckets;

    /**
     * Default constructor.
     */
    public PageTimestampHistogram() {
        this(DFLT_BUCKETS_INTERVAL, DFLT_BUCKETS_CNT);
    }

    /**
     * @param bucketsInterval Buckets interval.
     * @param bucketsCnt Buckets count.
     */
    PageTimestampHistogram(long bucketsInterval, int bucketsCnt) {
        this.bucketsInterval = bucketsInterval;
        this.bucketsCnt = bucketsCnt + 1; // One extra (dummy) bucket is reserved to deal with races.
        // Reserve 1 sec, page ts can be slightly lower than currentTimeMillis, due to applied to ts mask.
        startTs = U.currentTimeMillis() - 1000L;
        upperBoundTs = startTs + bucketsInterval;
        lowerBoundTs = upperBoundTs - bucketsCnt * bucketsInterval;
        buckets = new AtomicLongArray(this.bucketsCnt);
    }

    /**
     * Increment count of pages by last access time.
     */
    public void increment(long ts) {
        add(ts, 1);
    }

    /**
     * Decrement count of pages by last access time.
     */
    public void decrement(long ts) {
        add(ts, -1);
    }

    /**
     * Gets hot/cold pages histogram.
     *
     * @return Tuple, where first item is array of bounds and second item is array of values.
     */
    public IgniteBiTuple<long[], long[]> histogram() {
        long curTs = U.currentTimeMillis();

        long upperBoundTs = this.upperBoundTs;

        if (curTs >= upperBoundTs) {
            shiftBuckets();

            upperBoundTs = this.upperBoundTs;
        }

        long[] res = new long[bucketsCnt];
        long[] bounds = new long[bucketsCnt];

        long lowerBoundTs;

        do {
            lowerBoundTs = this.lowerBoundTs;

            if (upperBoundTs - lowerBoundTs != (bucketsCnt - 1) * bucketsInterval) {
                // Buckets shift in progress, wait for completion.
                synchronized (this) {
                    upperBoundTs = this.upperBoundTs;
                    lowerBoundTs = this.lowerBoundTs;
                }
            }

            assert upperBoundTs - lowerBoundTs == (bucketsCnt - 1) * bucketsInterval :
                    "Unexpected buckets bounds [upperBoundTs" + upperBoundTs + ", lowerBoundTs" + lowerBoundTs + ']';

            int bucketIdx = bucketIdx(upperBoundTs); // Index of the dummy bucket.

            res[0] = outOfBoundsBucket.get() + buckets.get(bucketIdx);
            bounds[0] = Math.min(startTs, lowerBoundTs - bucketsInterval);

            for (int i = 1; i < bucketsCnt; i++) { // Starting from 1 (bucketIdx + 1 - index of the last backet).
                res[i] = buckets.get((bucketIdx + i) % bucketsCnt);
                bounds[i] = lowerBoundTs + (i - 1) * bucketsInterval;
            }
        }
        // Retry if buckets shift started concurrently, in other case we can lost some buckets values or calculate some
        // buckets twice.
        // We can't atomically get values without locking, so, there is still can be some inconsistence between buckets,
        // but no more then concurrently changed values by other threads.
        while (lowerBoundTs != this.lowerBoundTs);

        return new IgniteBiTuple<>(bounds, res);
    }

    /**
     * Gets buckets interval.
     */
    long bucketsInterval() {
        return bucketsInterval;
    }

    /**
     * Gets buckets count.
     */
    int bucketsCount() {
        return bucketsCnt;
    }

    /**
     * Gets start timestamp.
     */
    long startTs() {
        return startTs;
    }

    /**
     * Gets bucket index by timestamp.
     */
    private int bucketIdx(long ts) {
        assert ts >= startTs : "Unexpected timestamp [startTs=" + startTs + ", ts=" + ts + ']';

        return (int)((ts - startTs) / bucketsInterval) % bucketsCnt;
    }

    /**
     * Change count of pages in bucket by given timestamp.
     *
     * @param ts Timestamp.
     * @param val Value to add.
     */
    private void add(long ts, int val) {
        long curTs = U.currentTimeMillis();

        assert ts <= curTs : "Unexpected timestamp [curTs = " + curTs + ", ts=" + ts + ']';

        if (curTs >= upperBoundTs)
            shiftBuckets();

        if (ts < lowerBoundTs)
            outOfBoundsBucket.addAndGet(val);
        else {
            int idx = bucketIdx(ts);

            // There is a race between lowerBoundTs check and bucket modification, so we can modify dropped bucket
            // in some cases (no more than one bucket behind lowerBoundTs). Dummy bucket was reserved for this purpose
            // (to avoid interference of writes to dropped bucket and writes to most recent bucket).
            // Values from dummy bucket will be flushed to outOfBoundsBucket during next shift.
            buckets.addAndGet(idx, val);
        }
    }

    /**
     * Shift buckets to ensure that upper bound of the buckets array is always greater then current timestamp.
     */
    private synchronized void shiftBuckets() {
        long curTs = U.currentTimeMillis();

        long oldUpperBoundTs = upperBoundTs;

        // Double check under the lock.
        if (curTs < oldUpperBoundTs)
            return;

        int bucketsSinceLastShift = (int)((curTs - oldUpperBoundTs) / bucketsInterval) + 1;

        long newUpperBoundTs = oldUpperBoundTs + bucketsSinceLastShift * bucketsInterval;

        int bucketsToShift = Math.min(bucketsCnt, bucketsSinceLastShift);

        lowerBoundTs = newUpperBoundTs - (bucketsCnt - 1) * bucketsInterval;

        int shiftBucketIdx = bucketIdx(oldUpperBoundTs);

        // Move content of all dropped buckets (including dummy bucket) to the "out of bounds" bucket.
        for (int i = 0; i <= bucketsToShift; i++) {
            outOfBoundsBucket.addAndGet(buckets.getAndSet(shiftBucketIdx, 0));

            shiftBucketIdx = (shiftBucketIdx + 1) % bucketsCnt;
        }

        upperBoundTs = newUpperBoundTs;
    }
}
