/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.future;

import org.jdk8.backport.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Performance tests added to compare the same functionality in .Net.
 */
public class GridFutureQueueTest {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        int threads = 64;

        for (int i = 0; i < 3; i++)
            new QueueTest().testQueue(30000, threads);
    }

    /**
     */
    private static class Message {
        /** */
        public final long id;

        /**
         * @param id Message id.
         */
        Message(long id) {
            this.id = id;
        }
    }

    /**
     *
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor")
    private static class Future<T> extends GridFutureAdapter<T> {
        /** */
        private Message msg;

        /**
         * @param msg Message.
         */
        Future(Message msg) {
            this.msg = msg;
        }
    }

    /**
     *
     */
    private static class QueueTest {
        /** */
        private AtomicLong qSize = new AtomicLong();

        /** */
        private final ConcurrentLinkedDeque8<Future> queue = new ConcurrentLinkedDeque8<>();

        /** */
        private volatile boolean stop;

        /** */
        private final Object mux = new Object();

        /** */
        private AtomicLong cnt = new AtomicLong();

        /**
         * @param time Test time.
         * @param writers Number of writers threads.
         * @throws Exception If failed.
         */
        public void testQueue(long time, int writers) throws Exception {
            System.out.println("Start test [writers=" + writers + ", time=" + time + "]");

            Thread rdr = new Thread() {
                @SuppressWarnings({"InfiniteLoopStatement", "unchecked"})
                @Override public void run() {
                    try {
                        while (true) {
                            Future fut;

                            while ((fut = queue.poll()) != null) {
                                qSize.decrementAndGet();

                                fut.onDone(true);
                            }

                            long size = qSize.get();

                            if (size == 0) {
                                synchronized (mux) {
                                    while (queue.isEmpty()) {
                                        mux.wait();
                                    }
                                }
                            }
                        }
                    }
                    catch (InterruptedException ignore) {
                    }
                }
            };

            rdr.start();

            List<Thread> putThreads = new ArrayList<>();

            for (int i = 0; i < writers; i++) {
                putThreads.add(new Thread() {
                    @SuppressWarnings("CallToNotifyInsteadOfNotifyAll")
                    @Override public void run() {
                        try {
                            while (!stop) {
                                long id = cnt.incrementAndGet();

                                Future<Boolean> fut = new Future<Boolean>(new Message(id));

                                queue.offer(fut);

                                long size = qSize.incrementAndGet();

                                if (size == 1) {
                                    synchronized (mux) {
                                        mux.notify();
                                    }
                                }

                                Boolean res = fut.get();

                                if (!res) {
                                    System.out.println("Error");
                                }
                            }
                        }
                        catch (Exception e ) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            for (Thread t : putThreads)
                t.start();

            Thread.sleep(time);

            stop = true;

            for (Thread t : putThreads)
                t.join();

            rdr.interrupt();

            rdr.join();

            System.out.println("Total: " + cnt.get());

            System.gc();
            System.gc();
            System.gc();
        }
    }
}
