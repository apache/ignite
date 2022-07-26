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

package org.apache.ignite.internal.util;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Rate limiter tests.
 */
public class BasicRateLimiterTest extends GridCommonAbstractTest {
    /**
     * Check change speed at runtime.
     */
    @Test
    public void checkSpeedLimitChange() throws Exception {
        int threadsCnt = 1;

        BasicRateLimiter limiter = new BasicRateLimiter(2);
        checkRate(limiter, 10, 1, threadsCnt);

        limiter.setRate(3);
        checkRate(limiter, 15, 1, threadsCnt);

        limiter.setRate(0.5);
        checkRate(limiter, 5, 1, threadsCnt);

        limiter.setRate(1_000);
        checkRate(limiter, 10_000, 1, threadsCnt);

        limiter.setRate(U.GB);
        checkRate(limiter, 8 * U.GB, U.KB, threadsCnt);
    }

    /**
     * Check change speed at runtime.
     */
    @Test
    public void checkSpeedLimitChangeMultithreaded() throws Exception {
        int threadsCnt = Runtime.getRuntime().availableProcessors();

        BasicRateLimiter limiter = new BasicRateLimiter(1_000);
        checkRate(limiter, 10_000, 1, threadsCnt);

        limiter.setRate(U.GB);
        checkRate(limiter, 8 * U.GB, U.KB, threadsCnt);
    }

    /**
     * Check that the rate can be set as unlimited.
     */
    @Test
    public void testUnlimitedRate() throws IgniteInterruptedCheckedException {
        BasicRateLimiter limiter = new BasicRateLimiter(0);
        limiter.acquire(Integer.MAX_VALUE);

        limiter.setRate(1);
        limiter.acquire(1);

        limiter.setRate(0);
        limiter.acquire(Integer.MAX_VALUE);
    }

    /**
     * Check the average rate of the limiter.
     *
     * @param limiter Rate limiter.
     * @param totalOps Number of operations.
     * @param blockSize Block size.
     * @param threads Number of threads.
     */
    private void checkRate(
        BasicRateLimiter limiter,
        long totalOps,
        long blockSize,
        int threads
    ) throws IgniteCheckedException, BrokenBarrierException, InterruptedException, TimeoutException {
        CyclicBarrier ready = new CyclicBarrier(threads + 1);
        AtomicLong cntr = new AtomicLong();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            ready.await();

            do {
                limiter.acquire(blockSize);
            }
            while (!Thread.currentThread().isInterrupted() && cntr.addAndGet(blockSize) < totalOps);

            return null;
        }, threads, "worker");

        ready.await(getTestTimeout(), TimeUnit.MILLISECONDS);

        long startTime = System.currentTimeMillis();

        fut.get(getTestTimeout());

        checkResult(cntr.get(), System.currentTimeMillis() - startTime, limiter.getRate(), threads);
    }

    /**
     * @param totalPermits Total operations.
     * @param timeSpent Total time.
     * @param rate Limit rate.
     * @param threads Number of threads.
     */
    private void checkResult(long totalPermits, long timeSpent, double rate, int threads) {
        double res = (double)timeSpent / 1000 / totalPermits * rate;

        log.info(String.format("Permits=%d, rate=%.2f, time=%d, threads=%d, error=%.2f%%",
            totalPermits, rate, timeSpent, threads, (1.0 - res) * 100));

        // Rate limiter aims for an average rate of permits per second.
        assertEquals(1, Math.round(res));
    }
}
