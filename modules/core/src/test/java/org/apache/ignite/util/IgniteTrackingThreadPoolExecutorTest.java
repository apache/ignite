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

package org.apache.ignite.util;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.thread.IgniteTrackingThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for tracking thread pool executor.
 */
public class IgniteTrackingThreadPoolExecutorTest extends TestCase {
    /** */
    private IgniteTrackingThreadPoolExecutor executor;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        int procs = Runtime.getRuntime().availableProcessors();

        executor = new IgniteTrackingThreadPoolExecutor("test", "default",
            procs * 2, procs * 2, 30_000, new LinkedBlockingQueue<>());
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        List<Runnable> runnables = executor.shutdownNow();

        assertEquals("Some tasks are not completed", 0, runnables.size());
    }

    /** */
    public void testSimple() throws IgniteCheckedException {
        doTest();
    }

    /** */
    public void testWithException() throws IgniteCheckedException {
        try {
            doTest(100, 2345, 5555, 90000);

            fail();
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();

            assertEquals(4, e.getSuppressed().length);
        }
    }

    /** */
    public void testReuse() throws IgniteCheckedException {
        long avg = 0;

        long warmUp = 30;

        int iters = 150;

        for (int i = 0; i < iters; i++) {
            long t1 = System.nanoTime();

            doTest();

            if (i >= warmUp)
                avg += System.nanoTime() - t1;

            executor.reset();
        }

        X.print("Average time per iteration: " + (avg / (iters - warmUp)) / 1000 / 1000. + " ms");
    }

    /** */
    private void doTest(@Nullable int... fails) throws IgniteCheckedException {
        if (fails != null)
            Arrays.sort(fails);

        LongAdder cnt = new LongAdder();

        int exp = 100_000;

        for (int i = 0; i < exp; i++) {
            final int finalI = i;
            executor.execute(new Runnable() {
                @Override public void run() {
                    if (fails != null && Arrays.binarySearch(fails, finalI) >= 0)
                        throw new IgniteException("Failed to add: " + finalI);
                    else
                        cnt.add(1);
                }
            });
        }

        executor.markInitialized();

        executor.awaitDone();

        assertEquals("Counter is not as expected", exp, cnt.sum());
    }
}
