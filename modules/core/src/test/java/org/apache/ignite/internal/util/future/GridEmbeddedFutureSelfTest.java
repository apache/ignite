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

package org.apache.ignite.internal.util.future;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.C2;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.CacheConfiguration.DFLT_MAX_CONCURRENT_ASYNC_OPS;

/**
 * Tests grid embedded future use cases.
 */
public class GridEmbeddedFutureSelfTest extends GridCommonAbstractTest {
    /**
     * Test kernal context.
     */
    private GridTestKernalContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ctx = new GridTestKernalContext(log);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFutureChain() throws Exception {
        GridFutureAdapter<Integer> fut = new GridFutureAdapter<>();

        IgniteInternalFuture<Integer> cur = fut;

        for (int i = 0; i < DFLT_MAX_CONCURRENT_ASYNC_OPS; i++) {
            cur = new GridEmbeddedFuture<>(cur,
                new IgniteBiClosure<Integer, Exception, IgniteInternalFuture<Integer>>() {
                    @Override public IgniteInternalFuture<Integer> apply(Integer o, Exception e) {
                        return new GridFinishedFuture<>(o);
                    }
                });
        }

        fut.onDone(1);
    }

    /**
     * Test embedded future completes when internal future finishes.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    public void testFutureCompletesCorrectly() throws Exception {
        List<Throwable> list = Arrays.asList(
            null,
            new RuntimeException("Test runtime exception (should be ignored)."),
            new IllegalStateException("Test illegal state exception (should be ignored)."),
            new Error("Test error (should be ignored)."),
            new AssertionError("Test assertion (should be ignored)."),
            new OutOfMemoryError("Test out of memory error (should be ignored)."),
            new StackOverflowError("Test stack overflow error (should be ignored).")
        );

        for (final Throwable x : list) {
            // Original future.
            final GridFutureAdapter<Integer> origFut = new GridFutureAdapter<>();

            // Embedded future to test.
            GridEmbeddedFuture<Double, Integer> embFut = new GridEmbeddedFuture<>(
                new C2<Integer, Exception, Double>() {
                    @Override public Double apply(Integer val, Exception e) {
                        if (x instanceof Error)
                            throw (Error)x;

                        if (x instanceof RuntimeException)
                            throw (RuntimeException)x;

                        assert x == null : "Only runtime exceptions and errors applicable for testing exception: " + x;

                        return null;
                    }
                },
                origFut);

            assertFalse("Expect original future is not complete.", origFut.isDone());
            assertFalse("Expect embedded future is not complete.", embFut.isDone());

            // Finish original future in separate thread.
            Thread t = new Thread() {
                @Override public void run() {
                    origFut.onDone(100);
                }
            };

            t.start();
            t.join();

            assertTrue("Expect original future is complete.", origFut.isDone());
            assertTrue("Expect embedded future is complete.", embFut.isDone());

            // Wait for embedded future completes.
            try {
                embFut.get(1, SECONDS);
            }
            catch (IgniteFutureTimeoutCheckedException e) {
                fail("Failed with timeout exception: " + e);
            }
            catch (IgniteCheckedException e) {
                info("Failed with unhandled exception (normal behaviour): " + e);

                assertSame(x, e.getCause(x.getClass()));
            }
            catch (Error e) {
                info("Failed with unhandled error (normal behaviour): " + e);

                assertSame(x, e);
            }
        }
    }
}