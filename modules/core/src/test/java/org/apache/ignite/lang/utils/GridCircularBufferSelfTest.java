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

import java.util.Deque;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridCircularBufferSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testMutliThreaded2() throws Exception {
        int size = 256 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final AtomicInteger itemGen = new AtomicInteger();

        info("Created buffer: " + buf);

        final int iterCnt = 10_000;
        final Deque<Integer> evictedQ = new ConcurrentLinkedDeque<>();
        final Deque<Integer> putQ = new ConcurrentLinkedDeque<>();

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
        assert evictedQ.size() == putQ.size();

        info("Buffer: " + buf);
    }
}
