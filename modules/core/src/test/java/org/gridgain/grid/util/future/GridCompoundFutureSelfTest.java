/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;

import java.util.*;

/**
 * Tests compound future contracts.
 */
public class GridCompoundFutureSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testMarkInitialized() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>();

        for (int i = 0; i < 5; i++) {
            IgniteFuture<Boolean> part = new GridFinishedFuture<>(ctx, true);

            part.syncNotify(true);

            fut.add(part);
        }

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        fut.markInitialized();

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompleteOnReducer() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(ctx, CU.boolReducer());

        List<GridFutureAdapter<Boolean>> futs = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>(ctx, true);

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        for (int i = 0; i < 3; i++) {
            futs.get(i).onDone(true);

            assertFalse(fut.isDone());
        }

        futs.get(3).onDone(false);

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompleteOnException() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(ctx, CU.boolReducer());

        List<GridFutureAdapter<Boolean>> futs = new ArrayList<>(5);

        for (int i = 0; i < 5; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>(ctx, true);

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        assertFalse(fut.isDone());
        assertFalse(fut.isCancelled());

        for (int i = 0; i < 3; i++) {
            futs.get(i).onDone(true);

            assertFalse(fut.isDone());
        }

        futs.get(3).onDone(new GridException("Test message"));

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentCompletion() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(ctx, CU.boolReducer());

        final ConcurrentLinkedDeque8<GridFutureAdapter<Boolean>> futs =
            new ConcurrentLinkedDeque8<>();

        for (int i = 0; i < 1000; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>(ctx, true);

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        IgniteFuture<?> complete = multithreadedAsync(new Runnable() {
            @Override public void run() {
                GridFutureAdapter<Boolean> part;

                while ((part = futs.poll()) != null)
                    part.onDone(true);
            }
        }, 20);

        complete.get();

        assertTrue(fut.isDone());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentRandomCompletion() throws Exception {
        GridTestKernalContext ctx = new GridTestKernalContext(log);

        GridCompoundFuture<Boolean, Boolean> fut = new GridCompoundFuture<>(ctx, CU.boolReducer());

        final ConcurrentLinkedDeque8<GridFutureAdapter<Boolean>> futs =
            new ConcurrentLinkedDeque8<>();

        for (int i = 0; i < 1000; i++) {
            GridFutureAdapter<Boolean> part = new GridFutureAdapter<>(ctx, true);

            fut.add(part);

            futs.add(part);
        }

        fut.markInitialized();

        IgniteFuture<?> complete = multithreadedAsync(new Runnable() {
            @Override public void run() {
                GridFutureAdapter<Boolean> part;

                Random rnd = new Random();

                while ((part = futs.poll()) != null) {
                    int op = rnd.nextInt(10);

                    if (op < 8)
                        part.onDone(true);
                    else if (op == 8)
                        part.onDone(false);
                    else
                        part.onDone(new GridException("TestMessage"));
                }
            }
        }, 20);

        complete.get();

        assertTrue(fut.isDone());
    }
}
