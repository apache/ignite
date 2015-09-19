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

package org.apache.ignite.lang.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentLinkedDeque8;
import org.jsr166.LongAdder8;

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
        final LongAdder8 cnt = new LongAdder8();
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
        final LongAdder8 cnt = new LongAdder8();
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
        final LongAdder8 cnt = new LongAdder8();
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
        final LongAdder8 cnt = new LongAdder8();
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