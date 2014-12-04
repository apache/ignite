/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;

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

        fut.onDone(new GridException("TestMessage"));

        final GridFutureAdapter<String> callFut1 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut1.get();
            }
        }, GridException.class, "TestMessage");

        fut = new GridFutureAdapter<>();

        fut.onDone("test", new GridException("TestMessage"));

        final GridFutureAdapter<String> callFut2 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut2.get();
            }
        }, GridException.class, "TestMessage");

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
        }, GridFutureCancelledException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridFutureAdapter<String> fut = new GridFutureAdapter<>();

                fut.onCancelled();

                fut.onDone();

                return fut.get();
            }
        }, GridFutureCancelledException.class, null);
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
            fut.listenAsync(new CI1<IgniteFuture<String>>() {
                @Override public void apply(IgniteFuture<String> t) {
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

        fut.listenAsync(new CI1<IgniteFuture<String>>() {
            @Override public void apply(IgniteFuture<String> t) {
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
                fut.listenAsync(new CI1<IgniteFuture<String>>() {
                    @Override public void apply(IgniteFuture<String> t) {
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

            fut.listenAsync(new CI1<IgniteFuture<String>>() {
                @Override public void apply(IgniteFuture<String> t) {
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
        final CX1<IgniteFuture<Object>, Object> passThrough = new CX1<IgniteFuture<Object>, Object>() {
            @Override public Object applyx(IgniteFuture<Object> f) throws GridException {
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
            IgniteFuture<Object> chain = fut.chain(passThrough);

            assertFalse(fut.isDone());
            assertFalse(chain.isDone());

            try {
                chain.get(20);

                fail("Expects timeout exception.");
            }
            catch (GridFutureTimeoutException e) {
                info("Expected timeout exception: " + e.getMessage());
            }

            fut.onDone("result");

            assertEquals("result", chain.get(1));

            // Test exception re-thrown.

            fut = new GridFutureAdapter<>(ctx);
            chain = fut.chain(passThrough);

            fut.onDone(new GridEmptyProjectionException("test exception"));

            try {
                chain.get();

                fail("Expects failed with exception.");
            }
            catch (GridEmptyProjectionException e) {
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
        catch (GridFutureTimeoutException e) {
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
        catch (GridFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }

        try {
            cancelled.get(1000);

            assert false;
        }
        catch (GridFutureCancelledException e) {
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
        catch (GridFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }

        try {
            cancelled.get(1000);

            assert false;
        }
        catch (GridFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }
    }
}
