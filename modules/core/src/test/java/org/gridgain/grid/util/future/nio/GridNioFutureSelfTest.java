/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future.nio;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for NIO future.
 */
public class GridNioFutureSelfTest extends GridCommonAbstractTest {

    /**
     * @throws Exception If failed.
     */
    public void testOnDone() throws Exception {
        GridNioFutureImpl<String> fut = new GridNioFutureImpl<>();

        fut.onDone();

        assertNull(fut.get());

        fut = new GridNioFutureImpl<>();

        fut.onDone("test");

        assertEquals("test", fut.get());

        fut = new GridNioFutureImpl<>();

        fut.onDone(new GridException("TestMessage"));

        final GridNioFutureImpl<String> callFut1 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut1.get();
            }
        }, GridException.class, "TestMessage");

        fut = new GridNioFutureImpl<>();

        fut.onDone("test", new GridException("TestMessage"));

        final GridNioFuture<String> callFut2 = fut;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return callFut2.get();
            }
        }, GridException.class, "TestMessage");

        fut = new GridNioFutureImpl<>();

        fut.onDone("test");

        fut.onCancelled();

        assertEquals("test", fut.get());
    }

    /**
     * @throws Exception
     */
    public void testOnCancelled() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridNioFutureImpl<String> fut = new GridNioFutureImpl<>();

                fut.onCancelled();

                return fut.get();
            }
        }, IgniteFutureCancelledException.class, null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridNioFutureImpl<String> fut = new GridNioFutureImpl<>();

                fut.onCancelled();

                fut.onDone();

                return fut.get();
            }
        }, IgniteFutureCancelledException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testListenSyncNotify() throws Exception {
        GridNioFutureImpl<String> fut = new GridNioFutureImpl<>();

        int lsnrCnt = 10;

        final CountDownLatch latch = new CountDownLatch(lsnrCnt);

        final Thread runThread = Thread.currentThread();

        final AtomicReference<Exception> err = new AtomicReference<>();

        for (int i = 0; i < lsnrCnt; i++) {
            fut.listenAsync(new CI1<GridNioFuture<String>>() {
                @Override public void apply(GridNioFuture<String> t) {
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

        fut.listenAsync(new CI1<GridNioFuture<String>>() {
            @Override public void apply(GridNioFuture<String> t) {
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
    public void testGet() throws Exception {
        GridNioFutureImpl<Object> unfinished = new GridNioFutureImpl<>();
        GridNioFutureImpl<Object> finished = new GridNioFutureImpl<>();
        GridNioFutureImpl<Object> cancelled = new GridNioFutureImpl<>();

        finished.onDone("Finished");

        cancelled.onCancelled();

        try {
            unfinished.get(50);

            assert false;
        }
        catch (IgniteFutureTimeoutException e) {
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
        catch (IgniteFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }

        try {
            cancelled.get(1000);

            assert false;
        }
        catch (IgniteFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }

    }
}
