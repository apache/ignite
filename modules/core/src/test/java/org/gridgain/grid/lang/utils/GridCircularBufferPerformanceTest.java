/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridCircularBufferPerformanceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testThroughput() throws Exception {
        int size = 256 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final LongAdder cnt = new LongAdder();
        final AtomicBoolean finished = new AtomicBoolean();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    U.sleep(5000);

                    info("Ops/sec: " + cnt.sumThenReset() / 5);
                }

                return null;
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        buf.add(1);

                        cnt.increment();
                    }

                    return null;
                }
            },
            8);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDequeueThroughput() throws Exception {

        final ConcurrentLinkedDeque8<Integer> buf = new ConcurrentLinkedDeque8<>();
        final LongAdder cnt = new LongAdder();
        final AtomicBoolean finished = new AtomicBoolean();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    U.sleep(5000);

                    info("Ops/sec: " + cnt.sumThenReset() / 5);
                }

                return null;
            }
        }, 1);

        final int size = 256 * 1024;

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        buf.add(1);

                        if (buf.sizex() > size)
                            buf.poll();

                        cnt.increment();
                    }

                    return null;
                }
            },
            8);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testArrayBlockingQueueThroughput() throws Exception {
        final int size = 256 * 1024;

        final ArrayBlockingQueue<Integer> buf = new ArrayBlockingQueue<>(size);
        final LongAdder cnt = new LongAdder();
        final AtomicBoolean finished = new AtomicBoolean();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    U.sleep(5000);

                    info("Ops/sec: " + cnt.sumThenReset() / 5);
                }

                return null;
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get()) {
                        buf.add(1);

                        buf.poll();

                        cnt.increment();
                    }

                    return null;
                }
            },
            8);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAdderThroughput() throws Exception {
        final int size = 256 * 1024;

        final ArrayBlockingQueue<Integer> buf = new ArrayBlockingQueue<>(size);
        final LongAdder cnt = new LongAdder();
        final AtomicBoolean finished = new AtomicBoolean();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    U.sleep(5000);

                    info("Ops/sec: " + cnt.sumThenReset() / 5);
                }

                return null;
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get())
                        cnt.increment();

                    return null;
                }
            },
            8);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicLongThroughput() throws Exception {
        final int size = 256 * 1024;

        final ArrayBlockingQueue<Integer> buf = new ArrayBlockingQueue<>(size);
        final AtomicLong cnt = new AtomicLong();
        final AtomicBoolean finished = new AtomicBoolean();

        multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    U.sleep(5000);

                    info("Ops/sec: " + cnt.getAndSet(0) / 5);
                }

                return null;
            }
        }, 1);

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!finished.get())
                        cnt.incrementAndGet();

                    return null;
                }
            },
            8);

        info("Buffer: " + buf);
    }
}
