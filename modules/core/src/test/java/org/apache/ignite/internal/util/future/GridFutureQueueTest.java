/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util.future;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

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
        private final Deque<Future> queue = new ConcurrentLinkedDeque<>();

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
                                    while (queue.isEmpty())
                                        mux.wait();
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

                                if (!res)
                                    System.out.println("Error");
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
