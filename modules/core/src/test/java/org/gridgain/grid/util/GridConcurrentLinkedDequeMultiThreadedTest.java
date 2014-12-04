/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jdk8.backport.ConcurrentLinkedDeque8.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for {@link ConcurrentLinkedDeque8}.
 */
public class GridConcurrentLinkedDequeMultiThreadedTest extends GridCommonAbstractTest {
    /** */
    private static final Random RND = new Random();

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    public void testQueueMultiThreaded() throws Exception {
        final AtomicBoolean done = new AtomicBoolean();

        final ConcurrentLinkedDeque8<Byte> queue = new ConcurrentLinkedDeque8<>();

        // Poll thread.
        IgniteFuture<?> pollFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get())
                        try {
                            queue.poll();
                        }
                        catch (Throwable t) {
                            error("Error in poll thread.", t);

                            done.set(true);
                        }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-poll"
        );

        // Producer thread.
        IgniteFuture<?> prodFut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    info("Thread started.");

                    while (!done.get()) {
                        Node<Byte> n = queue.addx((byte)1);

                        if (RND.nextBoolean())
                            queue.unlinkx(n);
                    }

                    info("Thread finished.");

                    return null;
                }
            },
            5,
            "queue-prod"
        );

        Thread.sleep(2 * 60 * 1000);


        done.set(true);

        pollFut.get();
        prodFut.get();
    }
}
