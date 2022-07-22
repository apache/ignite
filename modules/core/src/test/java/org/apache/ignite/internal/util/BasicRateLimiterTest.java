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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Rate limiter tests.
 */
public class BasicRateLimiterTest {
    /**
     * Check change speed at runtime.
     */
    @Test
    public void checkSpeedLimitChange() throws IgniteInterruptedCheckedException {
        BasicRateLimiter limiter = new BasicRateLimiter(2);
        checkRate(limiter, 10, 1);

        limiter.setRate(3);
        checkRate(limiter, 15, 1);

        limiter.setRate(0.5);
        checkRate(limiter, 5, 1);

        limiter.setRate(U.GB);
        checkRate(limiter, 8 * U.GB, U.KB);
    }

    /**
     * Check the average rate of the limiter.
     *
     * @param limiter Rate limiter.
     * @param totalOps Number of operations.
     * @param blockSize Block size.
     */
    private void checkRate(BasicRateLimiter limiter, long totalOps, long blockSize) throws IgniteInterruptedCheckedException {
        double permitsPerSec = limiter.getRate();
        long startTime = System.currentTimeMillis();

        for (long i = 0; i < totalOps; i += blockSize)
            limiter.acquire(blockSize);

        long timeSpent = System.currentTimeMillis() - startTime;

        // Rate limiter aims for an average rate of permits per second.
        assertEquals(1, Math.round((double)timeSpent / 1000 / totalOps * permitsPerSec));
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
     * Check rate limit with multiple threads.
     */
    @Test
    public void checkLimitMultithreaded() throws Exception {
        int permitsPerSec = 1_000;
        int totalOps = 10_000;

        BasicRateLimiter limiter = new BasicRateLimiter(permitsPerSec);

        int threads = Runtime.getRuntime().availableProcessors();

        CyclicBarrier ready = new CyclicBarrier(threads + 1);

        AtomicInteger cntr = new AtomicInteger();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
            ready.await();

            do {
                limiter.acquire(1);
            }
            while (!Thread.currentThread().isInterrupted() && cntr.incrementAndGet() < totalOps);

            return null;
        }, threads, "worker");

        ready.await();

        long startTime = System.currentTimeMillis();

        fut.get();

        long timeSpent = System.currentTimeMillis() - startTime;

        // Rate limiter aims for an average rate of permits per second.
        assertEquals(1, Math.round((double)timeSpent / 1000 / totalOps * permitsPerSec));
    }
}
