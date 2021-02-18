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

import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridTestClockTimer;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test PageTimestampHistogram class.
 */
public class PageTimestampHistogramTest extends GridCommonAbstractTest {
    /** Mock for current time */
    private static final AtomicLong curTime = new AtomicLong(System.currentTimeMillis());

    /** Test time supplier. */
    private static final LongSupplier timeSupplier = curTime::get;

    /** */
    @BeforeClass
    public static void beforeClass() {
        GridTestClockTimer.timeSupplier(timeSupplier);
    }

    /** */
    @AfterClass
    public static void afterClass() {
        GridTestClockTimer.timeSupplier(GridTestClockTimer.DFLT_TIME_SUPPLIER);
    }

    /** */
    @Test
    public void testConcurrentUpdate() throws Exception {
        PageTimestampHistogram histogram = new PageTimestampHistogram();

        long interval = histogram.bucketsInterval();
        int bucketsCnt = histogram.bucketsCount();
        int threadCnt = 20;
        int iterations = 1000;

        long startTs = curTime.get();

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterations; i++) {
                long ts = addCurrentTime(interval);

                // Update current data.
                histogram.increment(ts);

                // Update historical data around buckets lower bound.
                for (int j = bucketsCnt - 10; j < bucketsCnt + 10; j++) {
                    long ts1 = ts - j * interval;

                    if (ts1 <= startTs)
                        break;

                    histogram.increment(ts1);
                    histogram.decrement(ts1);
                }
            }
        }, threadCnt, "histogram-updater");

        assertEquals(threadCnt * iterations, Arrays.stream(histogram.histogram().get2()).sum());
    }

    /** */
    @Test
    public void testConcurrentHistogram() throws Exception {
        PageTimestampHistogram histogram = new PageTimestampHistogram();

        long interval = histogram.bucketsInterval();
        int bucketsCnt = histogram.bucketsCount();
        int threadCnt = 20;
        int valPerBucket = 1000;
        int iterations = 1000;

        // Initial fill.
        for (int i = 0; i < bucketsCnt; i++) {
            long ts = addCurrentTime(interval);

            for (int j = 0; j < valPerBucket; j++)
                histogram.increment(ts);
        }

        assertEquals(valPerBucket * bucketsCnt, Arrays.stream(histogram.histogram().get2()).sum());

        GridTestUtils.runMultiThreaded(() -> {
            for (int i = 0; i < iterations; i++) {
                long ts = addCurrentTime(interval);

                for (int j = 0; j < valPerBucket; j++) {
                    histogram.increment(ts);
                    histogram.decrement(ts - j * interval);
                }

                long sum = Arrays.stream(histogram.histogram().get2()).sum();

                // Check that no buckets were lost during concurrent calculation.
                assertTrue("Unexpected pages count " + sum, sum >= valPerBucket * bucketsCnt);
            }
        }, threadCnt, "histogram-updater");

        assertEquals(valPerBucket * bucketsCnt, Arrays.stream(histogram.histogram().get2()).sum());
    }

    /** */
    @Test
    public void testConcurrentLowerBoundBucketUpdate() throws Exception {
        PageTimestampHistogram histogram = new PageTimestampHistogram();

        long interval = histogram.bucketsInterval();
        int bucketsCnt = histogram.bucketsCount();
        int threadCnt = 20;
        int iterations = 1000;

        CyclicBarrier barrier = new CyclicBarrier(threadCnt + 1);

        addCurrentTime(interval * bucketsCnt);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                for (int i = 0; i < iterations; i++) {
                    barrier.await(1, TimeUnit.SECONDS);

                    long ts = curTime.get() - interval * (bucketsCnt - 2); // 1 dummy bucket + 1 shifted bucket.

                    barrier.await(1, TimeUnit.SECONDS);

                    histogram.increment(ts);

                    // Maximize probability of collision between buckets shift and buckets update.
                    for (int j = 0; j < 10; j++) {
                        histogram.decrement(ts);
                        histogram.increment(ts);
                    }
                }
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }, threadCnt, "histogram-updater");

        for (int i = 0; i < iterations; i++) {
            barrier.await(1, TimeUnit.SECONDS);

            assertEquals(i * threadCnt, Arrays.stream(histogram.histogram().get2()).sum());
            assertEquals(i * threadCnt, histogram.histogram().get2()[0]);

            barrier.await(1, TimeUnit.SECONDS);

            histogram.increment(curTime.get());
            histogram.decrement(curTime.get());

            addCurrentTime(interval);

            long[] hist = histogram.histogram().get2();

            assertTrue("Unexpected pages count " + hist[0] + ", expected between " + (i * threadCnt) + " and " +
                (i + 1) * threadCnt, hist[0] >= i * threadCnt && hist[0] <= (i + 1) * threadCnt);

            for (int j = 1; j < hist.length; j++)
                assertEquals(0, hist[j]);
        }

        fut.get();
    }

    /** */
    @Test
    public void testShiftOneBucket() {
        PageTimestampHistogram histogram = new PageTimestampHistogram();

        long interval = histogram.bucketsInterval();
        int bucketsCnt = histogram.bucketsCount();

        long ts = histogram.startTs();

        addCurrentTime(ts - curTime.get() + interval - 1);

        histogram.increment(ts);
        histogram.increment(ts + 1);
        histogram.increment(ts + interval - 1);

        assertEquals(3, histogram.histogram().get2()[bucketsCnt - 1]);

        addCurrentTime(1);

        assertEquals(0, histogram.histogram().get2()[bucketsCnt - 1]);
        assertEquals(3, histogram.histogram().get2()[bucketsCnt - 2]);

        ts = curTime.get();

        addCurrentTime(interval - 1);

        histogram.increment(ts);
        histogram.increment(ts + 1);
        histogram.increment(ts + interval / 2);
        histogram.increment(ts + interval - 1);

        assertEquals(4, histogram.histogram().get2()[bucketsCnt - 1]);
        assertEquals(3, histogram.histogram().get2()[bucketsCnt - 2]);

        addCurrentTime(1);

        assertEquals(0, histogram.histogram().get2()[bucketsCnt - 1]);
        assertEquals(4, histogram.histogram().get2()[bucketsCnt - 2]);
        assertEquals(3, histogram.histogram().get2()[bucketsCnt - 3]);
    }

    /** */
    @Test
    public void testShiftMoreThanOneBucket() {
        PageTimestampHistogram histogram = new PageTimestampHistogram();

        long interval = histogram.bucketsInterval();
        int bucketsCnt = histogram.bucketsCount();

        long ts = curTime.get();

        histogram.increment(ts);

        assertEquals(1, histogram.histogram().get2()[bucketsCnt - 1]);

        ts = addCurrentTime(interval);

        histogram.increment(ts);

        assertEquals(1, histogram.histogram().get2()[bucketsCnt - 1]);
        assertEquals(1, histogram.histogram().get2()[bucketsCnt - 2]);

        ts = addCurrentTime(interval * (bucketsCnt - 2));

        histogram.increment(ts);

        assertEquals(1, histogram.histogram().get2()[bucketsCnt - 1]);
        assertEquals(1, histogram.histogram().get2()[1]);
        assertEquals(1, histogram.histogram().get2()[0]);

        for (int i = -1; i <= 1; i++) {
            ts = addCurrentTime(interval * (bucketsCnt + i));

            histogram.increment(ts);

            assertEquals(1, histogram.histogram().get2()[bucketsCnt - 1]);
            assertEquals(4 + i, histogram.histogram().get2()[0]);
        }

        addCurrentTime(interval * bucketsCnt);

        // Check shift without modification.
        assertEquals(6, histogram.histogram().get2()[0]);
    }

    /**
     * Add value to U.currentTimeMillis().
     *
     * @param time Time.
     */
    private static long addCurrentTime(long time) {
        long res = curTime.addAndGet(time);

        GridTestClockTimer.update();

        return res;
    }
}
