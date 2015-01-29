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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
        GridFutureAdapter<String> fut = new GridFutureAdapter<>(new GridTestKernalContext(log), true);

        int lsnrCnt = 10;

        final CountDownLatch latch = new CountDownLatch(lsnrCnt);

        final Thread runThread = Thread.currentThread();

        final AtomicReference<Exception> err = new AtomicReference<>();

        for (int i = 0; i < lsnrCnt; i++) {
            fut.listenAsync(new CI1<IgniteInternalFuture<String>>() {
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

        fut.listenAsync(new CI1<IgniteInternalFuture<String>>() {
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
    public void testListenAsyncNotify() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        ctx.config().setExecutorService(Executors.newFixedThreadPool(1));
        ctx.config().setSystemExecutorService(Executors.newFixedThreadPool(1));

        ctx.add(new GridClosureProcessor(ctx));

        ctx.start();

        try {
            GridFutureAdapter<String> fut = new GridFutureAdapter<>(ctx, false);

            int lsnrCnt = 10;

            final CountDownLatch latch = new CountDownLatch(lsnrCnt);

            final Thread runThread = Thread.currentThread();

            final AtomicReference<Exception> err = new AtomicReference<>();

            for (int i = 0; i < lsnrCnt; i++) {
                fut.listenAsync(new CI1<IgniteInternalFuture<String>>() {
                    @Override public void apply(IgniteInternalFuture<String> t) {
                        if (Thread.currentThread() == runThread)
                            err.compareAndSet(null, new Exception("Wrong notification thread: " +
                                Thread.currentThread()));

                        latch.countDown();
                    }
                });
            }

            fut.onDone();

            latch.await();

            if (err.get() != null)
                throw err.get();

            final CountDownLatch doneLatch = new CountDownLatch(1);

            err.set(null);

            fut.listenAsync(new CI1<IgniteInternalFuture<String>>() {
                @Override public void apply(IgniteInternalFuture<String> t) {
                    if (Thread.currentThread() == runThread)
                        err.compareAndSet(null, new Exception("Wrong notification thread: " + Thread.currentThread()));

                    doneLatch.countDown();
                }
            });

            doneLatch.await();

            if (err.get() != null)
                throw err.get();
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
    @SuppressWarnings("ErrorNotRethrown")
    public void testChaining() throws Exception {
        final CX1<IgniteInternalFuture<Object>, Object> passThrough = new CX1<IgniteInternalFuture<Object>, Object>() {
            @Override public Object applyx(IgniteInternalFuture<Object> f) throws IgniteCheckedException {
                return f.get();
            }
        };

        final GridTestKernalContext ctx = new GridTestKernalContext(log);

        ctx.config().setExecutorService(Executors.newFixedThreadPool(1));
        ctx.config().setSystemExecutorService(Executors.newFixedThreadPool(1));

        ctx.add(new GridClosureProcessor(ctx));

        ctx.start();

        try {
            // Test result returned.

            GridFutureAdapter<Object> fut = new GridFutureAdapter<>(ctx);
            IgniteInternalFuture<Object> chain = fut.chain(passThrough);

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

            fut = new GridFutureAdapter<>(ctx);
            chain = fut.chain(passThrough);

            fut.onDone(new ClusterGroupEmptyCheckedException("test exception"));

            try {
                chain.get();

                fail("Expects failed with exception.");
            }
            catch (ClusterGroupEmptyCheckedException e) {
                info("Expected exception: " + e.getMessage());
            }

            // Test error re-thrown.

            fut = new GridFutureAdapter<>(ctx);
            chain = fut.chain(passThrough);

            try {
                fut.onDone(new StackOverflowError("test error"));

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
        finally {
            ctx.stop(false);
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

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        GridFutureAdapter<Object> unfinished = new GridFutureAdapter<>();
        GridFutureAdapter<Object> finished = new GridFutureAdapter<>();
        GridFutureAdapter<Object> cancelled = new GridFutureAdapter<>();

        finished.onDone("Finished");

        cancelled.onCancelled();

        GridByteArrayOutputStream baos = new GridByteArrayOutputStream();

        ObjectOutputStream out = new ObjectOutputStream(baos);

        out.writeObject(unfinished);
        out.writeObject(finished);
        out.writeObject(cancelled);

        out.close();

        ObjectInputStream in = new ObjectInputStream(new GridByteArrayInputStream(baos.internalArray(),
            0, baos.size()));

        unfinished = (GridFutureAdapter<Object>)in.readObject();
        finished = (GridFutureAdapter<Object>)in.readObject();
        cancelled = (GridFutureAdapter<Object>)in.readObject();

        try {
            unfinished.get();

            assert false;
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            unfinished.get(1000);

            assert false;
        }
        catch (IllegalStateException e) {
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
