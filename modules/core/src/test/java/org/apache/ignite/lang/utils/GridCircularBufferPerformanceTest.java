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

package org.apache.ignite.lang.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.deque.FastSizeDeque;
import org.junit.Test;

/**
 *
 */
public class GridCircularBufferPerformanceTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testDequeueThroughput() throws Exception {

        final FastSizeDeque<Integer> buf = new FastSizeDeque<>(new ConcurrentLinkedDeque<>());
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
    @Test
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
    @Test
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
    @Test
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
