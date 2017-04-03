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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

/**
 *
 */
public class IgniteFutureImplTest extends GridCommonAbstractTest {
    /** Context thread name. */
    private static final String CTX_THREAD_NAME = "test-async";

    /** Custom thread name. */
    private static final String CUSTOM_THREAD_NAME = "test-custom-async";

    /** Test executor. */
    private ExecutorService ctxExec;

    /** Custom executor. */
    private ExecutorService customExec;

    /** Context. */
    private GridKernalContextImpl ctx;

    /** {@inheritDoc} */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    @Override protected void beforeTest() throws Exception {
        ctxExec = createExecutor(CTX_THREAD_NAME);
        customExec = createExecutor(CUSTOM_THREAD_NAME);

        ctx = new GridKernalContextImpl(){
            @Override public ExecutorService getExecutorService() {
                return ctxExec;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.shutdownNow(getClass(), ctxExec, log);
        U.shutdownNow(getClass(), customExec, log);

        ctxExec = null;
        customExec = null;
        ctx = null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFutureGet() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        assertFalse(fut.isDone());

        assertTrue(fut.startTime() > 0);

        U.sleep(100);

        assertTrue(fut.duration() > 0);

        fut0.onDone("test");

        assertEquals("test", fut.get());

        assertTrue(fut.isDone());

        assertTrue(fut.duration() > 0);

        long dur0 = fut.duration();

        U.sleep(100);

        assertEquals(dur0, fut.duration());

        assertEquals("test", fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFutureException() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        final IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        assertFalse(fut.isDone());

        assertTrue(fut.startTime() > 0);

        U.sleep(100);

        assertTrue(fut.duration() > 0);

        IgniteCheckedException err0 = new IgniteCheckedException("test error");

        fut0.onDone(err0);

        IgniteException err = (IgniteException)GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteException.class, "test error");

        assertEquals(err0, err.getCause());

        assertTrue(fut.isDone());

        assertTrue(fut.duration() > 0);

        long dur0 = fut.duration();

        U.sleep(100);

        assertEquals(dur0, fut.duration());

        err = (IgniteException)GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteException.class, null);

        assertEquals(err0, err.getCause());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFutureIgniteException() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        final IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        IgniteException err0 = new IgniteException("test error");

        fut0.onDone(err0);

        IgniteException err = (IgniteException)GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteException.class, "test error");

        assertEquals(err0, err);
    }

    /**
     * @throws Exception If failed.
     */
    public void testListeners() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        final AtomicInteger lsnr1Cnt = new AtomicInteger();

        IgniteInClosure<? super IgniteFuture<String>> lsnr1 = new CI1<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> fut) {
                assertEquals("test", fut.get());

                lsnr1Cnt.incrementAndGet();
            }
        };

        final AtomicInteger lsnr2Cnt = new AtomicInteger();

        IgniteInClosure<? super IgniteFuture<String>> lsnr2 = new CI1<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> fut) {
                assertEquals("test", fut.get());

                lsnr2Cnt.incrementAndGet();
            }
        };

        assertFalse(fut.isDone());

        fut.listen(lsnr1);
        fut.listen(lsnr2);

        U.sleep(100);

        assertEquals(0, lsnr1Cnt.get());
        assertEquals(0, lsnr2Cnt.get());

        fut0.onDone("test");

        assertEquals(1, lsnr1Cnt.get());
        assertEquals(1, lsnr2Cnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenersOnError() throws Exception {
        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

            final IgniteException err0 = new IgniteException("test error");

            final AtomicBoolean passed = new AtomicBoolean();

            IgniteInClosure<? super IgniteFuture<String>> lsnr1 = new CI1<IgniteFuture<String>>() {
                @Override public void apply(IgniteFuture<String> fut) {
                    try {
                        fut.get();

                        fail();
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err);

                        passed.set(true);
                    }
                }
            };

            fut.listen(lsnr1);

            fut0.onDone(err0);

            assertTrue(passed.get());
        }

        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

            final IgniteCheckedException err0 = new IgniteCheckedException("test error");

            final AtomicBoolean passed = new AtomicBoolean();

            IgniteInClosure<? super IgniteFuture<String>> lsnr1 = new CI1<IgniteFuture<String>>() {
                @Override public void apply(IgniteFuture<String> fut) {
                    try {
                        fut.get();

                        fail();
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err.getCause());

                        passed.set(true);
                    }
                }
            };

            fut.listen(lsnr1);

            fut0.onDone(err0);

            assertTrue(passed.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncListeners() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        final CountDownLatch latch1 = new CountDownLatch(2);

        IgniteInClosure<? super IgniteFuture<String>> lsnr1 = createAsyncListener(latch1, CTX_THREAD_NAME, null);
        IgniteInClosure<? super IgniteFuture<String>> lsnr2 = createAsyncListener(latch1, CUSTOM_THREAD_NAME, null);

        assertFalse(fut.isDone());

        fut.listenAsync(lsnr1);
        fut.listenAsync(lsnr2, customExec);

        U.sleep(100);

        assertEquals(2, latch1.getCount());

        fut0.onDone("test");

        assert latch1.await(1, TimeUnit.SECONDS) : latch1.getCount();

        final CountDownLatch latch2 = new CountDownLatch(2);

        IgniteInClosure<? super IgniteFuture<String>> lsnr3 = createAsyncListener(latch2, CTX_THREAD_NAME, null);
        IgniteInClosure<? super IgniteFuture<String>> lsnr4 = createAsyncListener(latch2, CUSTOM_THREAD_NAME, null);

        fut.listenAsync(lsnr3);
        fut.listenAsync(lsnr4, customExec);

        assert latch1.await(1, TimeUnit.SECONDS) : latch2.getCount();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncListenersOnError() throws Exception {
        checkAsyncListenerOnError(new IgniteException("Test exception"));
        checkAsyncListenerOnError(new IgniteCheckedException("Test checked exception"));
    }

    /**
     * @param err0 Test exception.
     */
    private void checkAsyncListenerOnError(Exception err0) throws InterruptedException {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        final CountDownLatch latch1 = new CountDownLatch(2);

        IgniteInClosure<? super IgniteFuture<String>> lsnr1 = createAsyncListener(latch1, CTX_THREAD_NAME, err0);
        IgniteInClosure<? super IgniteFuture<String>> lsnr2 = createAsyncListener(latch1, CUSTOM_THREAD_NAME, err0);

        fut.listenAsync(lsnr1);
        fut.listenAsync(lsnr2, customExec);

        assertEquals(2, latch1.getCount());

        fut0.onDone(err0);

        assert latch1.await(1, TimeUnit.SECONDS);

        final CountDownLatch latch2 = new CountDownLatch(2);

        IgniteInClosure<? super IgniteFuture<String>> lsnr3 = createAsyncListener(latch2, CTX_THREAD_NAME, err0);
        IgniteInClosure<? super IgniteFuture<String>> lsnr4 = createAsyncListener(latch2, CUSTOM_THREAD_NAME, err0);

        fut.listenAsync(lsnr3);
        fut.listenAsync(lsnr4, customExec);

        assert latch2.await(1, TimeUnit.SECONDS);
    }

    /**
     * @param latch Latch.
     */
    @NotNull private CI1<IgniteFuture<String>> createAsyncListener(
        final CountDownLatch latch,
        final String threadName,
        final Exception err
    ) {
        return new CI1<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> fut) {
                try {
                    String tname = Thread.currentThread().getName();

                    assert tname.contains(threadName) : tname;

                    assertEquals("test", fut.get());

                    if (err != null)
                        fail();
                }
                catch (IgniteException e) {
                    if (err != null)
                        assertExpectedException(e, err);
                    else
                        throw e;
                }
                finally {
                    latch.countDown();
                }
            }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testChain() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        IgniteFuture<Integer> chained = fut.chain(new C1<IgniteFuture<String>, Integer>() {
            @Override public Integer apply(IgniteFuture<String> fut) {
                return Integer.valueOf(fut.get());
            }
        });

        assertFalse(chained.isDone());

        assertTrue(chained.startTime() > 0);

        U.sleep(100);

        assertTrue(chained.duration() > 0);

        final AtomicInteger lsnrCnt = new AtomicInteger();

        chained.listen(new CI1<IgniteFuture<Integer>>() {
            @Override public void apply(IgniteFuture<Integer> fut) {
                assertEquals(10, (int)fut.get());

                lsnrCnt.incrementAndGet();
            }
        });

        fut0.onDone("10");

        assertTrue(chained.isDone());

        assertTrue(chained.duration() > 0);

        long dur0 = chained.duration();

        U.sleep(100);

        assertEquals(dur0, chained.duration());

        assertEquals(10, (int)chained.get());

        assertEquals(1, lsnrCnt.get());

        assertTrue(fut.isDone());

        assertEquals("10", fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testChainError() throws Exception {
        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

            final IgniteException err0 = new IgniteException("test error");

            final AtomicBoolean chainedPassed = new AtomicBoolean();

            IgniteFuture<Integer> chained = fut.chain(new C1<IgniteFuture<String>, Integer>() {
                @Override public Integer apply(IgniteFuture<String> fut) {
                    try {
                        fut.get();

                        fail();

                        return -1;
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err);

                        chainedPassed.set(true);

                        throw err;
                    }
                }
            });

            final AtomicBoolean lsnrPassed = new AtomicBoolean();

            IgniteInClosure<? super IgniteFuture<Integer>> lsnr1 = new CI1<IgniteFuture<Integer>>() {
                @Override public void apply(IgniteFuture<Integer> fut) {
                    try {
                        fut.get();

                        fail();
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err);

                        lsnrPassed.set(true);
                    }
                }
            };

            chained.listen(lsnr1);

            fut0.onDone(err0);

            assertTrue(chainedPassed.get());

            assertTrue(lsnrPassed.get());

            try {
                chained.get();

                fail();
            }
            catch (IgniteException err) {
                assertEquals(err0, err);
            }

            try {
                fut.get();

                fail();
            }
            catch (IgniteException err) {
                assertEquals(err0, err);
            }
        }

        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

            final IgniteCheckedException err0 = new IgniteCheckedException("test error");

            final AtomicBoolean chainedPassed = new AtomicBoolean();

            IgniteFuture<Integer> chained = fut.chain(new C1<IgniteFuture<String>, Integer>() {
                @Override public Integer apply(IgniteFuture<String> fut) {
                    try {
                        fut.get();

                        fail();

                        return -1;
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err.getCause());

                        chainedPassed.set(true);

                        throw err;
                    }
                }
            });

            final AtomicBoolean lsnrPassed = new AtomicBoolean();

            IgniteInClosure<? super IgniteFuture<Integer>> lsnr1 = new CI1<IgniteFuture<Integer>>() {
                @Override public void apply(IgniteFuture<Integer> fut) {
                    try {
                        fut.get();

                        fail();
                    }
                    catch (IgniteException err) {
                        assertEquals(err0, err.getCause());

                        lsnrPassed.set(true);
                    }
                }
            };

            chained.listen(lsnr1);

            fut0.onDone(err0);

            assertTrue(chainedPassed.get());

            assertTrue(lsnrPassed.get());

            try {
                chained.get();

                fail();
            }
            catch (IgniteException err) {
                assertEquals(err0, err.getCause());
            }

            try {
                fut.get();

                fail();
            }
            catch (IgniteException err) {
                assertEquals(err0, err.getCause());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testChainAsync() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        IgniteFuture<Integer> chained1 = fut.chainAsync(new C1<IgniteFuture<String>, Integer>() {
            @Override public Integer apply(IgniteFuture<String> fut) {
                assertEquals(CTX_THREAD_NAME, Thread.currentThread().getName());

                return Integer.valueOf(fut.get());
            }
        });

        IgniteFuture<Integer> chained2 = fut.chainAsync(new C1<IgniteFuture<String>, Integer>() {
            @Override public Integer apply(IgniteFuture<String> fut) {
                assertEquals(CUSTOM_THREAD_NAME, Thread.currentThread().getName());

                return Integer.valueOf(fut.get());
            }
        }, customExec);

        assertFalse(chained1.isDone());
        assertFalse(chained2.isDone());

        assertTrue(chained1.startTime() > 0);
        assertTrue(chained2.startTime() > 0);

        U.sleep(100);

        assertTrue(chained1.duration() > 0);
        assertTrue(chained2.duration() > 0);

        final CountDownLatch latch = new CountDownLatch(2);

        // Listeners will be completed in the same thread as chained future.
        chained1.listen(new CI1<IgniteFuture<Integer>>() {
            @Override public void apply(IgniteFuture<Integer> fut) {
                assertEquals(CTX_THREAD_NAME, Thread.currentThread().getName());
                assertEquals(10, (int)fut.get());

                latch.countDown();
            }
        });

        chained2.listen(new CI1<IgniteFuture<Integer>>() {
            @Override public void apply(IgniteFuture<Integer> fut) {
                assertEquals(CUSTOM_THREAD_NAME, Thread.currentThread().getName());
                assertEquals(10, (int)fut.get());

                latch.countDown();
            }
        });

        fut0.onDone("10");

        // Chained future will be completed asynchronously.
        chained1.get(100, TimeUnit.MILLISECONDS);
        chained2.get(100, TimeUnit.MILLISECONDS);

        assertTrue(chained1.isDone());
        assertTrue(chained2.isDone());

        assertTrue(chained1.duration() > 0);
        assertTrue(chained2.duration() > 0);

        assertEquals(10, (int)chained1.get());
        assertEquals(10, (int)chained2.get());

        assert latch.await(100, TimeUnit.MILLISECONDS);

        assertTrue(fut.isDone());

        assertEquals("10", fut.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testChainAsyncOnError() throws Exception {
        checkChainedOnError(new IgniteException("Test exception"));
        checkChainedOnError(new IgniteCheckedException("Test checked exception"));
    }

    /**
     * @param err Exception.
     * @throws Exception If failed.
     */
    private void checkChainedOnError(final Exception err) throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0, ctx);

        // Each chain callback will be invoked in specific executor.
        IgniteFuture<Integer> chained1 = fut.chainAsync(new C1<IgniteFuture<String>, Integer>() {
            @Override public Integer apply(IgniteFuture<String> fut) {
                assertEquals(CTX_THREAD_NAME, Thread.currentThread().getName());

                try {
                    fut.get();

                    fail();
                }
                catch (IgniteException e) {
                    assertExpectedException(e, err);

                    throw e;
                }

                return -1;
            }
        });

        IgniteFuture<Integer> chained2 = fut.chainAsync(new C1<IgniteFuture<String>, Integer>() {
            @Override public Integer apply(IgniteFuture<String> fut) {
                assertEquals(CUSTOM_THREAD_NAME, Thread.currentThread().getName());

                try {
                    fut.get();

                    fail();
                }
                catch (IgniteException e) {
                    assertExpectedException(e, err);

                    throw e;
                }

                return -1;
            }
        }, customExec);

        assertFalse(chained1.isDone());
        assertFalse(chained2.isDone());
        assertFalse(fut.isDone());

        assertTrue(chained1.startTime() > 0);
        assertTrue(chained2.startTime() > 0);

        U.sleep(100);

        assertTrue(chained1.duration() > 0);
        assertTrue(chained2.duration() > 0);

        final CountDownLatch latch = new CountDownLatch(2);

        // Listeners will be completed in the same thread as chained future.
        chained1.listen(new CI1<IgniteFuture<Integer>>() {
            @Override public void apply(IgniteFuture<Integer> fut) {
                try {
                    assertEquals(CTX_THREAD_NAME, Thread.currentThread().getName());

                    fut.get();

                    fail();
                }
                catch (IgniteException e) {
                    assertExpectedException(e, err);
                }
                finally {
                    latch.countDown();
                }
            }
        });

        chained2.listen(new CI1<IgniteFuture<Integer>>() {
            @Override public void apply(IgniteFuture<Integer> fut) {
                try {
                    assertEquals(CUSTOM_THREAD_NAME, Thread.currentThread().getName());

                    fut.get();

                    fail();
                }
                catch (IgniteException e) {
                    assertExpectedException(e, err);
                }
                finally {
                    latch.countDown();
                }
            }
        });

        fut0.onDone(err);

        assertExceptionThrown(err, chained1);
        assertExceptionThrown(err, chained2);
        assertExceptionThrown(err, fut);

        assertTrue(chained1.isDone());
        assertTrue(chained2.isDone());
        assertTrue(fut.isDone());

        assertTrue(chained1.duration() > 0);
        assertTrue(chained2.duration() > 0);

        assert latch.await(100, TimeUnit.MILLISECONDS);
    }

    /**
     * @param err Expected exception.
     * @param fut Future.
     */
    private void assertExceptionThrown(Exception err, IgniteFuture<?> fut) {
        try {
            fut.get();

            fail();
        }
        catch (IgniteException e) {
            assertExpectedException(e, err);
        }
    }

    /**
     * @param e Actual exception.
     * @param err Expected exception.
     */
    private void assertExpectedException(IgniteException e, Exception err) {
        if (err instanceof IgniteException)
            assertEquals(err, e);
        else
            assertEquals(err, e.getCause());
    }

    /**
     * @param name Name.
     */
    @NotNull private ExecutorService createExecutor(final String name) {
        return Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override public Thread newThread(@NotNull Runnable r) {
                Thread t = new Thread(r);

                t.setName(name);

                return t;
            }
        });
    }
}