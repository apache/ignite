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

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import junit.framework.TestCase;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteTaskTrackingThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for tracking thread pool executor.
 */
public class IgniteTaskTrackingThreadPoolExecutorTest extends TestCase {
    /** */
    private IgniteTaskTrackingThreadPoolExecutor executor;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        int procs = Runtime.getRuntime().availableProcessors();

        executor = new IgniteTaskTrackingThreadPoolExecutor("test", "default",
            procs * 2, procs * 2, 30_000, new LinkedBlockingQueue<>(), GridIoPolicy.UNDEFINED, (t, e) -> {
                // No-op.
            });
    }

    /** {@inheritDoc} */
    @Override protected void tearDown() throws Exception {
        List<Runnable> runnables = executor.shutdownNow();

        assertEquals("Some tasks are not completed", 0, runnables.size());
    }

    /** */
    public void testSimple() throws IgniteCheckedException {
        doTest(null);
    }

    /** */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testWithException() throws IgniteCheckedException {
        int fail = 5555;

        try {
            doTest(fail);

            fail();
        }
        catch (Throwable t) {
            TestException cause = (TestException)X.getCause(t);

            assertEquals(fail, cause.idx);
        }

        AtomicReference<Throwable> err = U.field(executor, "err");
        err.set(null);

        executor.awaitDone();
    }

    /** */
    public void testReuse() throws IgniteCheckedException {
        long avg = 0;

        long warmUp = 30;

        int iters = 150;

        for (int i = 0; i < iters; i++) {
            long t1 = System.nanoTime();

            doTest(null);

            if (i >= warmUp)
                avg += System.nanoTime() - t1;

            executor.reset();
        }

        X.print("Average time per iteration: " + (avg / (iters - warmUp)) / 1000 / 1000. + " ms");
    }

    /** */
    private void doTest(@Nullable Integer fail) throws IgniteCheckedException {
        LongAdder cnt = new LongAdder();

        int exp = 100_000;

        for (int i = 0; i < exp; i++) {
            final int finalI = i;
            executor.execute(() -> {
                if (fail != null && fail == finalI)
                    throw new TestException(finalI);
                else
                    cnt.add(1);
            });
        }

        executor.markInitialized();

        executor.awaitDone();

        assertEquals("Counter is not as expected", exp, cnt.sum());
    }

    /** */
    private static class TestException extends RuntimeException {
        /** */
        final int idx;

        /**
         * @param idx Index.
         */
        public TestException(int idx) {
            this.idx = idx;
        }
    }
}
