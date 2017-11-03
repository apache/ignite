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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureCancelledCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests grid future adapter use cases.
 */
public class GridFutureAdapterSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testOnDone() throws Exception {
        GridFutureAdapter<String> fut = new GridFutureAdapter<>();

        fut.onDone();

        assertNull(fut.get());

        fut = new GridFutureAdapter<>();

        fut.onDone("test");

        assertEquals("test", fut.get());

        fut = new GridFutureAdapter<>();

        fut.onDone(new IgniteCheckedException("TestMessage"));

        final GridFutureAdapter<String> callFut1 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut1.get();
            }
        }, IgniteCheckedException.class, "TestMessage");

        fut = new GridFutureAdapter<>();

        fut.onDone("test", new IgniteCheckedException("TestMessage"));

        final GridFutureAdapter<String> callFut2 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut2.get();
            }
        }, IgniteCheckedException.class, "TestMessage");

        fut = new GridFutureAdapter<>();

        fut.onDone("test");

        fut.onCancelled();

        assertEquals("test", fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testOnCancelled() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                fut.onCancelled();

                return fut.get();
            }
        }, IgniteFutureCancelledCheckedException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                fut.onCancelled();

                fut.onDone();

                return fut.get();
            }
        }, IgniteFutureCancelledCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenSyncNotify() throws Exception {
        GridFutureAdapter<String> fut = new GridFutureAdapter<>();

        int lsnrCnt = 10;

        final CountDownLatch latch = new CountDownLatch(lsnrCnt);

        final Thread runThread = Thread.currentThread();

        final AtomicReference<Exception> err = new AtomicReference<>();

        for (int i = 0; i < lsnrCnt; i++) {
            fut.listen(new CI1<IgniteInternalFuture<String>>() {
                @Override public void apply(IgniteInternalFuture<String> t) {
                    if (Thread.currentThread() != runThread)
                        err.compareAndSet(null, new Exception("Wrong notification thread: " + Thread.currentThread()));

                    latch.countDown();
                }
            });
        }

        fut.onDone();

        assertEquals(0, latch.getCount());

        if (err.get() != null)
            throw err.get();

        final AtomicBoolean called = new AtomicBoolean();

        err.set(null);

        fut.listen(new CI1<IgniteInternalFuture<String>>() {
            @Override public void apply(IgniteInternalFuture<String> t) {
                if (Thread.currentThread() != runThread)
                    err.compareAndSet(null, new Exception("Wrong notification thread: " + Thread.currentThread()));

                called.set(true);
            }
        });

        assertTrue(called.get());

        if (err.get() != null)
            throw err.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenNotify() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        ctx.setExecutorService(Executors.newFixedThreadPool(1));
        ctx.setSystemExecutorService(Executors.newFixedThreadPool(1));

        ctx.add(new PoolProcessor(ctx));
        ctx.add(new GridClosureProcessor(ctx));

        ctx.start();

        try {
            GridFutureAdapter<String> fut = new GridFutureAdapter<>();

            int lsnrCnt = 10;

            final CountDownLatch latch = new CountDownLatch(lsnrCnt);

            final Thread runThread = Thread.currentThread();

            for (int i = 0; i < lsnrCnt; i++) {
                fut.listen(new CI1<IgniteInternalFuture<String>>() {
                    @Override public void apply(IgniteInternalFuture<String> t) {
                        assert Thread.currentThread() == runThread;

                        latch.countDown();
                    }
                });
            }

            fut.onDone();

            latch.await();

            final CountDownLatch doneLatch = new CountDownLatch(1);

            fut.listen(new CI1<IgniteInternalFuture<String>>() {
                @Override public void apply(IgniteInternalFuture<String> t) {
                    assert Thread.currentThread() == runThread;

                    doneLatch.countDown();
                }
            });

            assert doneLatch.getCount() == 0;

            doneLatch.await();
        }
        finally {
            ctx.stop(false);
        }
    }

    /**
     * Test futures chaining.
     *
     * @throws Exception In case of any exception.
     */
    public void testChaining() throws Exception {
        checkChaining(null);

        ExecutorService exec = Executors.newFixedThreadPool(1);

        try {
            checkChaining(exec);

            GridFinishedFuture<Integer> fut = new GridFinishedFuture<>(1);

            IgniteInternalFuture<Object> chain = fut.chain(new CX1<IgniteInternalFuture<Integer>, Object>() {
                @Override public Object applyx(IgniteInternalFuture<Integer> fut) throws IgniteCheckedException {
                    return fut.get() + 1;
                }
            }, exec);

            assertEquals(2, chain.get());
        }
        finally {
            exec.shutdown();
        }
    }

    /**
     * @param exec Executor for chain callback.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void checkChaining(ExecutorService exec) throws Exception {
        final CX1<IgniteInternalFuture<Object>, Object> passThrough = new CX1<IgniteInternalFuture<Object>, Object>() {
            @Override public Object applyx(IgniteInternalFuture<Object> f) throws IgniteCheckedException {
                return f.get();
            }
        };

        GridFutureAdapter<Object> fut = new GridFutureAdapter<>();
        IgniteInternalFuture<Object> chain = exec != null ? fut.chain(passThrough, exec) : fut.chain(passThrough);

        assertFalse(fut.isDone());
        assertFalse(chain.isDone());

        try {
            chain.get(20);

            fail("Expects timeout exception.");
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            info("Expected timeout exception: " + e.getMessage());
        }

        fut.onDone("result");

        assertEquals("result", chain.get(1));

        // Test exception re-thrown.

        fut = new GridFutureAdapter<>();
        chain = exec != null ? fut.chain(passThrough, exec) : fut.chain(passThrough);

        fut.onDone(new ClusterGroupEmptyCheckedException("test exception"));

        try {
            chain.get();

            fail("Expects failed with exception.");
        }
        catch (ClusterGroupEmptyCheckedException e) {
            info("Expected exception: " + e.getMessage());
        }

        // Test error re-thrown.

        fut = new GridFutureAdapter<>();
        chain = exec != null ? fut.chain(passThrough, exec) : fut.chain(passThrough);

        try {
            fut.onDone(new StackOverflowError("test error"));

            if (exec == null)
                fail("Expects failed with error.");
        }
        catch (StackOverflowError e) {
            info("Expected error: " + e.getMessage());
        }

        try {
            chain.get();

            fail("Expects failed with error.");
        }
        catch (StackOverflowError e) {
            info("Expected error: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        GridFutureAdapter<Object> unfinished = new GridFutureAdapter<>();
        GridFutureAdapter<Object> finished = new GridFutureAdapter<>();
        GridFutureAdapter<Object> cancelled = new GridFutureAdapter<>();

        finished.onDone("Finished");

        cancelled.onCancelled();

        try {
            unfinished.get(50);

            assert false;
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            info("Caught expected exception: " + e);
        }

        Object o = finished.get();

        assertEquals("Finished", o);

        o = finished.get(1000);

        assertEquals("Finished", o);

        try {
            cancelled.get();

            assert false;
        }
        catch (IgniteFutureCancelledCheckedException e) {
            info("Caught expected exception: " + e);
        }

        try {
            cancelled.get(1000);

            assert false;
        }
        catch (IgniteFutureCancelledCheckedException e) {
            info("Caught expected exception: " + e);
        }

    }
}