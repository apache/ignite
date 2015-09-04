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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 *
 */
public class GridCircularBufferSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testCreation() {
        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(-2);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(0);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(5);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(8);

        info("Created buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleThreaded() throws Exception {
        int size = 8;
        int iterCnt = size * 10;

        GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);

        info("Created buffer: " + buf);

        Integer lastEvicted = null;

        for (int i = 0; i < iterCnt; i++) {
            Integer evicted = buf.add(i);

            info("Evicted: " + evicted);

            if (i >= size) {
                assert evicted != null;

                if (lastEvicted == null) {
                    lastEvicted = evicted;

                    continue;
                }

                assert lastEvicted + 1 == evicted : "Fail [lastEvicted=" + lastEvicted + ", evicted=" + evicted + ']';

                lastEvicted = evicted;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutliThreaded() throws Exception {
        int size = 32 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final AtomicInteger itemGen = new AtomicInteger();

        info("Created buffer: " + buf);

        final int iterCnt = 1_000_000;

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < iterCnt; i++) {
                    int item = itemGen.getAndIncrement();

                    buf.add(item);
                }

                return null;
            }
        }, 32);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutliThreaded2() throws Exception {
        int size = 256 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final AtomicInteger itemGen = new AtomicInteger();

        info("Created buffer: " + buf);

        final int iterCnt = 10_000;
        final ConcurrentLinkedDeque8<Integer> evictedQ = new ConcurrentLinkedDeque8<>();
        final ConcurrentLinkedDeque8<Integer> putQ = new ConcurrentLinkedDeque8<>();

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < iterCnt; i++) {
                        int item = itemGen.getAndIncrement();

                        putQ.add(item);

                        Integer evicted = buf.add(item);

                        if (evicted != null)
                            evictedQ.add(evicted);
                    }

                    return null;
                }
            },
            8);

        evictedQ.addAll(buf.items());

        assert putQ.containsAll(evictedQ);
        assert evictedQ.sizex() == putQ.sizex();

        info("Buffer: " + buf);
    }
}