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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * TODO FIXME add multithreaded test.
 */
public class PartitionUpdateCounterTest extends GridCommonAbstractTest {
    public void testPrimaryMode() {
        for (int i = 0; i < 1000; i++)
            doTestPrimaryMode(2, 6, 2, 10, 3, 1, 5, 4);
    }

    private void doTestPrimaryMode(long... reservations) {
        PartitionUpdateCounter pc = new PartitionUpdateCounter(log);

        long[] ctrs = new long[reservations.length];

        for (int i = 0; i < reservations.length; i++)
            ctrs[i] = pc.reserve(reservations[i]);

        List<T2<Long, Long>> tmp = new ArrayList<>();

        for (int i = 0; i < ctrs.length; i++)
            tmp.add(new T2<>(ctrs[i], reservations[i]));

        Collections.shuffle(tmp);

        for (T2<Long, Long> objects : tmp)
            pc.release(objects.get1(), objects.get2());

        assertEquals(pc.get(), pc.hwm());

        assertEquals(Arrays.stream(reservations).sum(), pc.get());
    }

    public void testBackupMode() {
        PartitionUpdateCounter pc = new PartitionUpdateCounter(log);

        pc.update(0, 1);
        pc.update(2, 1);

        pc.update(5, 1);
        pc.update(6, 1);
        pc.update(7, 1);

        PartitionUpdateCounter pc2 = new PartitionUpdateCounter(log);

        pc2.update(7, 1);
        pc2.update(6, 1);
        pc2.update(5, 1);

        pc2.update(2, 1);
        pc2.update(0, 1);

        assertEquals(pc, pc2);
    }

    public void testBackupMode2() {
        int[][] updates = generateUpdates(1000, 5);

        List<int[]> tmp = new ArrayList<>();

        long expTotal = 0;

        for (int i = 0; i < updates.length; i++) {
            int[] pair = updates[i];

            tmp.add(pair);

            expTotal += pair[1];
        }

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
                assertTrue(pc0.holes().isEmpty());

                pc = pc0;
            }
        }
    }

    /**
     * @param cnt Count.
     */
    private int[][] generateUpdates(int cnt, int maxTxSize) {
        int[] ints = new Random().ints(cnt, 1, maxTxSize + 1).toArray();

        int[][] pairs = new int[cnt][2];

        int off = 0;

        for (int i = 0; i < ints.length; i++) {
            int val = ints[i];

            pairs[i][0] = off;
            pairs[i][1] = val;

            off += val;
        }

        return pairs;
    }
}
