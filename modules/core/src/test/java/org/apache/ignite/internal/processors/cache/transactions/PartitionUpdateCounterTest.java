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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Simple partition counter tests.
 */
@RunWith(JUnit4.class)
public class PartitionUpdateCounterTest extends GridCommonAbstractTest {
    /**
     * Test applying update multiple times in random order.
     */
    @Test
    public void testRandomUpdates() {
        List<int[]> tmp = generateUpdates(1000, 5);

        long expTotal = tmp.stream().mapToInt(pair -> pair[1]).sum();

        PartitionUpdateCounter pc = null;

        for (int i = 0; i < 100; i++) {
            Collections.shuffle(tmp);

            PartitionUpdateCounter pc0 = new PartitionUpdateCounter(log);

            for (int[] pair : tmp)
                pc0.update(pair[0], pair[1]);

            if (pc == null)
                pc = pc0;
            else {
                assertEquals(pc, pc0);
                assertEquals(expTotal, pc0.get());
                assertTrue(pc0.gaps().isEmpty());

                pc = pc0;
            }
        }
    }

    /**
     * Test if pc correctly reports stale (before current counter) updates.
     * This information is used for logging rollback records only once.
     */
    @Test
    public void testStaleUpdate() {
        PartitionUpdateCounter pc = new PartitionUpdateCounter(log);

        assertTrue(pc.update(0, 1));
        assertFalse(pc.update(0, 1));

        assertTrue(pc.update(2, 1));
        assertFalse(pc.update(2, 1));

        assertTrue(pc.update(1, 1));
        assertFalse(pc.update(1, 1));
    }

    /**
     * Test multithreaded updates of pc in various modes.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedModeMultithreaded() throws Exception {
        PartitionUpdateCounter pc = new PartitionUpdateCounter(log);

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            while(!stop.get()) {
                if (ThreadLocalRandom.current().nextBoolean())
                    pc.next();
                else {
                    long reserved = pc.reserve(1);

                    Thread.yield();

                    pc.update(reserved, 1);
                }
            }
        }, 4, "updater-thread");

        doSleep(10_000);

        stop.set(true);

        fut.get();

        log.info(pc.toString());

        assertTrue(pc.gaps().isEmpty());

        assertTrue(pc.get() == pc.reserved());
    }

    /**
     * @param cnt Count.
     */
    private List<int[]> generateUpdates(int cnt, int maxTxSize) {
        int[] ints = new Random().ints(cnt, 1, maxTxSize + 1).toArray();

        int off = 0;

        List<int[]> res = new ArrayList<>(cnt);

        for (int i = 0; i < ints.length; i++) {
            int val = ints[i];

            res.add(new int[] {off, val});

            off += val;
        }

        return res;
    }
}
