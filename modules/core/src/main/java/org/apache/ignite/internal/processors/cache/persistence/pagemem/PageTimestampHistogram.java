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
@SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized", "NonAtomicOperationOnVolatileField"})
public class PageTimestampHistogram {
    /** Default buckets interval in milliseconds. */
    public static final long DFLT_BUCKETS_INTERVAL = 60L * 60 * 1000; // 60 mins.

    /** Default buckets count. */
    public static final int DFLT_BUCKETS_CNT = 24;

    /** Buckets interval in milliseconds. */
    private long bucketsInterval;

    /** Buckets count. */
    private int bucketsCnt;

    /** Starting point for bucket index calculation. */
    private volatile long startTs;

    /** Lower bound for values stored in buckets array (including). */
    private volatile long lowerBoundTs;

    /** Upper bound for values stored in buckets array (excluding). */
    private volatile long upperBoundTs;

    /** Out of bounds bucket. Contain count of pages which have last access time beyond lowerBoundTs. */
    private final AtomicLong outOfBoundsBucket = new AtomicLong();

    /** Time of histogram creation. */
    private final long createTs = U.currentTimeMillis();

    /** Buckets holder. */
    private volatile AtomicLongArray buckets;

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
        reinit(bucketsInterval, bucketsCnt);

        // Reserve 1 sec, page ts can be slightly lower than currentTimeMillis, due to applied to ts mask. This
        // reservation mainly affects only tests (we can check buckets more predictevely).
        startTs -= 1000L;
        lowerBoundTs = startTs;
        upperBoundTs = startTs + bucketsInterval;
    }

    /**
     * @param bucketsInterval Buckets interval.
     * @param bucketsCnt Buckets count.
     */
    public synchronized void reinit(long bucketsInterval, int bucketsCnt) {
        startTs = U.currentTimeMillis();
        lowerBoundTs = startTs;
        upperBoundTs = startTs + bucketsInterval;

        this.bucketsInterval = bucketsInterval;
        this.bucketsCnt = bucketsCnt + 1; // One extra (dummy) bucket is reserved to deal with races.

        AtomicLongArray oldBuckets = buckets;

        buckets = new AtomicLongArray(this.bucketsCnt);

        if (oldBuckets != null) {
            for (int i = 0; i < oldBuckets.length(); i++)
                outOfBoundsBucket.addAndGet(oldBuckets.getAndSet(i, 0));
        }
    }

    /**
     * @param pagesCnt Total pages count.
     */
    public synchronized void reset(long pagesCnt) {
        reinit(bucketsInterval, bucketsCnt);

        outOfBoundsBucket.set(pagesCnt);
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
    public synchronized IgniteBiTuple<long[], long[]> histogram() {
        long curTs = U.currentTimeMillis();

        if (curTs >= upperBoundTs)
            shiftBuckets();

        int cnt = (int)((upperBoundTs - lowerBoundTs) / bucketsInterval) + 1;

        long[] res = new long[cnt];
        long[] bounds = new long[cnt];

        int dummyBucketIdx = dummyBucketIdx();

        res[0] = outOfBoundsBucket.get() + buckets.get(dummyBucketIdx);
        bounds[0] = createTs == lowerBoundTs ? createTs - bucketsInterval : createTs;

        for (int i = 1; i < cnt; i++) { // Starting from 1 (dummyBucketIdx + 1 = index of the first backet).
            res[i] = buckets.get((dummyBucketIdx + i) % bucketsCnt);
            bounds[i] = lowerBoundTs + (i - 1) * bucketsInterval;
        }

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
     *
     * Note: Since this method is not synchronized, in case of concurrent reinitialization we can get wrong value here
     * without external synchronyzation.
     */
    private int bucketIdx(long ts) {
        return (int)((ts - startTs) / bucketsInterval) % bucketsCnt;
    }

    /**
     * Gets index of dummy bucket.
     */
    private int dummyBucketIdx() {
        return (bucketIdx(lowerBoundTs) + bucketsCnt - 1) % bucketsCnt;
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
            AtomicLongArray buckets = this.buckets;
            int idx = bucketIdx(ts);

            if (ts <= startTs) { // Histogram was concurrently reinitialized.
                if (ts == startTs) {
                    synchronized (this) {
                        // We can't be sure about correct buckets variable without the lock here, but this is the rare
                        // case and will not affect performance much.
                        this.buckets.addAndGet(0, val);
                    }
                }
                else
                    outOfBoundsBucket.addAndGet(val);
            }
            else {
                // There is a race between lowerBoundTs check and bucket modification, so we can modify dropped bucket
                // in some cases (no more than one bucket behind lowerBoundTs). Dummy bucket was reserved for this purpose
                // (to avoid interference of writes to dropped bucket and writes to most recent bucket).
                // Values from dummy bucket will be flushed to outOfBoundsBucket during next shift.
                buckets.addAndGet(idx, val);

                if (buckets != this.buckets) {
                    // If histogram was concurrently reinitialized after bucket modification we can loose our change.
                    outOfBoundsBucket.addAndGet(buckets.getAndSet(idx, 0L));
                }
            }
        }
    }

    /**
     * Shift buckets to ensure that upper bound of the buckets array is always greater then current timestamp.
     */
    private synchronized void shiftBuckets() {
        long curTs = U.currentTimeMillis();

        long oldLowerBoundTs = lowerBoundTs;
        long oldUpperBoundTs = upperBoundTs;

        // Double check under the lock.
        if (curTs < oldUpperBoundTs)
            return;

        int bucketsSinceLastShift = (int)((curTs - oldUpperBoundTs) / bucketsInterval) + 1;

        long newUpperBoundTs = oldUpperBoundTs + bucketsSinceLastShift * bucketsInterval;

        long newLowerBoundTs = newUpperBoundTs - (bucketsCnt - 1) * bucketsInterval;

        if (newLowerBoundTs > oldLowerBoundTs) {
            int bucketsToShift = Math.min(bucketsCnt, (int)((newLowerBoundTs - oldLowerBoundTs) / bucketsInterval));

            int shiftBucketIdx = (bucketIdx(oldLowerBoundTs) + bucketsCnt - 1) % bucketsCnt; // Start with dummy bucket.

            // Move content of all dropped buckets (including dummy bucket) to the "out of bounds" bucket.
            for (int i = 0; i <= bucketsToShift; i++) {
                outOfBoundsBucket.addAndGet(buckets.getAndSet(shiftBucketIdx, 0));

                shiftBucketIdx = (shiftBucketIdx + 1) % bucketsCnt;
            }

            lowerBoundTs = newLowerBoundTs;
        }

        upperBoundTs = newUpperBoundTs;
    }
}
