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
import java.util.concurrent.Executor;
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
import org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests grid future adapter use cases.
 */
public class GridFutureAdapterSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut1.get();
            }
        }, IgniteCheckedException.class, "TestMessage");

        fut = new GridFutureAdapter<>();

        fut.onDone("test", new IgniteCheckedException("TestMessage"));

        final GridFutureAdapter<String> callFut2 = fut;

        assertThrows(log, new Callable<Object>() {
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
    @Test
    public void testOnCancelled() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                fut.onCancelled();

                return fut.get();
            }
        }, IgniteFutureCancelledCheckedException.class, null);

        assertThrows(log, new Callable<Object>() {
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
    @Test
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
    @Test
    public void testListenNotify() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        ctx.add(new NoOpIgniteSecurityProcessor(ctx));

        ctx.add(new PoolProcessor(ctx) {
            final ExecutorService execSvc = Executors.newSingleThreadExecutor(new IgniteThreadFactory("testscope", "exec-svc"));

            final ExecutorService sysExecSvc = Executors.newSingleThreadExecutor(new IgniteThreadFactory("testscope", "system-exec"));

            @Override public ExecutorService getSystemExecutorService() {
                return sysExecSvc;
            }

            @Override public ExecutorService getExecutorService() {
                return execSvc;
            }
        });

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
    @Test
    public void testChaining() throws Exception {
        checkChaining(null);

        ExecutorService exec = Executors.newSingleThreadExecutor(new IgniteThreadFactory("testscope", "grid-future-adapter-test-exec"));

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

        assertEquals("result", exec == null ? chain.get(1) : chain.get());

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
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testChainCompose() throws IgniteCheckedException {
        Executor[] executors = new Executor[] { null, Executors.newSingleThreadExecutor() };

        for (Executor exec : executors) {
            checkCompose(true, false, false, exec);
            checkCompose(false, false, false, exec);
            checkCompose(true, true, false, exec);
            checkCompose(false, true, false, exec);

            checkCompose(true, false, true, exec);
            checkCompose(false, false, true, exec);
            checkCompose(true, true, true, exec);
            checkCompose(false, true, true, exec);
        }

        checkComposeCancel();
    }

    /**
     * @param directOrder Whether to complete the futures in direct order. Direct order is: 1st is completed the future
     *                    on which {@link IgniteInternalFuture#chainCompose(IgniteClosure)} is called, 2nd is the future
     *                    returned by {@code doneCb} passed to  {@link IgniteInternalFuture#chainCompose(IgniteClosure)}.
     * @param withException Whether the futures are completed with exceptions.
     * @param cbException Whether the callback throws an exception instead of returning a future.
     * @param exec Executor.
     * @throws IgniteCheckedException If failed.
     */
    public void checkCompose(
        boolean directOrder,
        boolean withException,
        boolean cbException,
        Executor exec
    ) throws IgniteCheckedException {
        GridFutureAdapter<Object> fut0 = new GridFutureAdapter<>();
        GridFutureAdapter<Object> cbFut = new GridFutureAdapter<>();

        IgniteInternalFuture<Object> chainedFut = fut0.chainCompose(
            f -> {
                if (cbException)
                    throw new RuntimeException("cbException");
                else
                    return cbFut;
            },
            exec
        );

        assertFalse(chainedFut.isDone());

        Object res0 = withException ? new Exception("test0") : new Object();
        Object resCb = withException ? new Exception("testCb") : new Object();

        if (directOrder) {
            futureOnDone(fut0, res0);

            assertTrue(fut0.isDone());

            if (exec == null)
                assertEquals(cbException, chainedFut.isDone());

            futureOnDone(cbFut, resCb);
        }
        else {
            futureOnDone(cbFut, resCb);

            assertTrue(cbFut.isDone());

            if (exec == null)
                assertFalse(chainedFut.isDone());

            futureOnDone(fut0, res0);
        }

        boolean exceptionThrown = false;

        try {
            chainedFut.get(1000);
        }
        catch (Exception e) {
            exceptionThrown = true;

            if (cbException)
                assertEquals("cbException", e.getMessage());
            else if (withException)
                assertEquals("testCb", e.getMessage());
            else
                throw e;
        }

        assertEquals(withException || cbException, exceptionThrown);

        assertTrue(chainedFut.isDone());
    }

    /**
     * Completes the future exceptionally, if {@code res} is the instanse of {@link Throwable}.
     *
     * @param fut Future.
     * @param res Result.
     */
    private void futureOnDone(GridFutureAdapter<Object> fut, Object res) {
        if (res instanceof Throwable) {
            Throwable t = (Throwable)res;

            fut.onDone(t);
        }
        else
            fut.onDone(res);
    }

    /**
     * Check the behavior of compose future when inner future is cancelled.
     */
    private void checkComposeCancel() {
        GridFutureAdapter<Object> fut0 = new GridFutureAdapter<>();
        GridFutureAdapter<Object> fut1 = new GridFutureAdapter<>();

        IgniteInternalFuture<Object> fut2 = fut0.chainCompose(f -> fut1);

        fut0.onDone(new Object());

        fut1.onCancelled();

        assertTrue(fut1.isDone());
        assertTrue(fut1.isCancelled());

        assertThrows(log, () -> fut2.get(1000), IgniteFutureCancelledCheckedException.class, null);

        assertTrue(fut2.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
