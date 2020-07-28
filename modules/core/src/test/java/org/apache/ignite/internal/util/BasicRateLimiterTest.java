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
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * Rate limiter tests.
 */
public class BasicRateLimiterTest {
    /**
     * Check rate limit with multiple threads.
     */
    @Test
    public void checkLimitMultithreaded() throws Exception {
        int opsPerSec = 1000;
        int totalOps = 10_000;

        BasicRateLimiter limiter = new BasicRateLimiter(opsPerSec);

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

        assertEquals(totalOps / opsPerSec, SECONDS.convert(timeSpent, MILLISECONDS));
    }

    /**
     * Check that the average speed is limited correctly even if we are acquiring more permits than allowed per second.
     */
    @Test
    public void checkAcquireWithOverflow() throws IgniteInterruptedCheckedException {
        int permitsPerSec = 5;
        int permitsPerOp = permitsPerSec * 2;
        int totalOps = 5;

        BasicRateLimiter limiter = new BasicRateLimiter(5);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i <= totalOps; i++)
            limiter.acquire(permitsPerOp);

        long timeSpent = System.currentTimeMillis() - startTime;

        assertEquals(permitsPerOp * totalOps / permitsPerSec, SECONDS.convert(timeSpent, MILLISECONDS));
    }

    /**
     * Check change speed at runtime.
     */
    @Test
    public void checkSpeedLimitChange() throws IgniteInterruptedCheckedException {
        BasicRateLimiter limiter = new BasicRateLimiter(2);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 50; i++) {
            limiter.acquire(1);

            if (i == 10) {
                long timeSpent = System.currentTimeMillis() - startTime;

                assertEquals(5, SECONDS.convert(timeSpent, MILLISECONDS));

                limiter.setRate(8);
            }
        }

        long timeSpent = System.currentTimeMillis() - startTime;

        assertEquals(10, SECONDS.convert(timeSpent, MILLISECONDS));
    }
}
