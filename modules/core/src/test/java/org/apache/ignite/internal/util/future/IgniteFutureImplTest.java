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

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class IgniteFutureImplTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testFutureGet() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>();

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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

        final IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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

        final IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

        fut.syncNotify(true);

        assertTrue(fut.syncNotify());

        fut.syncNotify(false);

        assertFalse(fut.syncNotify());

        fut.concurrentNotify(true);

        assertTrue(fut.concurrentNotify());

        fut.concurrentNotify(false);

        assertFalse(fut.concurrentNotify());

        fut.syncNotify(true);

        assertTrue(fut.syncNotify());

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

        fut.listenAsync(lsnr1);
        fut.listenAsync(lsnr2);

        U.sleep(100);

        assertEquals(0, lsnr1Cnt.get());
        assertEquals(0, lsnr2Cnt.get());

        fut0.onDone("test");

        assertEquals(1, lsnr1Cnt.get());
        assertEquals(1, lsnr2Cnt.get());

        lsnr1Cnt.set(0);
        lsnr2Cnt.set(0);

        // Stop one listener.
        {
            fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            fut = new IgniteFutureImpl<>(fut0);

            fut.syncNotify(true);

            fut.listenAsync(lsnr1);
            fut.listenAsync(lsnr2);

            fut.stopListenAsync(lsnr2);

            fut0.onDone("test");

            assertEquals(1, lsnr1Cnt.get());
            assertEquals(0, lsnr2Cnt.get());

            lsnr1Cnt.set(0);
        }

        // Stop both listeners.
        {
            fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            fut = new IgniteFutureImpl<>(fut0);

            fut.syncNotify(true);

            fut.listenAsync(lsnr1);
            fut.listenAsync(lsnr2);

            fut.stopListenAsync(lsnr2, lsnr1);

            fut0.onDone("test");

            assertEquals(0, lsnr1Cnt.get());
            assertEquals(0, lsnr2Cnt.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenersOnError() throws Exception {
        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

            fut.syncNotify(true);

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

            fut.listenAsync(lsnr1);

            fut0.onDone(err0);

            assertTrue(passed.get());
        }

        {
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

            fut.syncNotify(true);

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

            fut.listenAsync(lsnr1);

            fut0.onDone(err0);

            assertTrue(passed.get());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testChain() throws Exception {
        GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

        IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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

        chained.listenAsync(new CI1<IgniteFuture<Integer>>() {
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
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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

            assertTrue(chained.syncNotify());

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

            chained.listenAsync(lsnr1);

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
            GridFutureAdapter<String> fut0 = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

            IgniteFutureImpl<String> fut = new IgniteFutureImpl<>(fut0);

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

            assertTrue(chained.syncNotify());

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

            chained.listenAsync(lsnr1);

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
}
