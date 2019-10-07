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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.junit.Test;

/**
 * Sweep removal tests.
 */
public class BPlusTreeSweepRemovalTest extends BPlusTreeReuseSelfTest {
    /** Number of entries to use. */
    private static int NUM_KEYS = 50_000;

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSweep() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;

        TestTree t = createTestTree(true);

        t.put(1L);
        t.put(2L);
        t.put(3L);
        t.put(4L);
        t.put(5L);
        t.put(6L);
        t.put(7L);
        t.put(8L);
        log.info(t.printTree());

        GridCursor<Void> cursor = t.sweep((tree, io, pageAddr, idx) -> io.getLookupRow(tree, pageAddr, idx) % 2 == 0);

        while (cursor.next())
            cursor.get();

        log.info(t.printTree());

        assertFalse(t.removex(2L));
        assertFalse(t.removex(4L));
        assertFalse(t.removex(6L));
        assertFalse(t.removex(8L));
        assertEquals(1L, t.findOne(1L).longValue());
        assertEquals(3L, t.findOne(3L).longValue());
        assertEquals(5L, t.findOne(5L).longValue());
        assertEquals(7L, t.findOne(7L).longValue());

        cursor = t.sweep((tree, io, pageAddr, idx) -> io.getLookupRow(tree, pageAddr, idx) % 2 == 1);

        while (cursor.next())
            cursor.get();

        log.info(t.printTree());

        assertTrue(t.isEmpty());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSweepLarge() throws IgniteCheckedException {
        TestTree t = createTestTree(true);

        BPlusTree.TreeRowClosure<Long, Long> clo = (tree, io, pageAddr, idx) ->
            io.getLookupRow(tree, pageAddr, idx) % 2 == 0;

        for (long k = 0L; k < NUM_KEYS; k++)
            t.putx(k);

        log.info("height=" + t.rootLevel());

        GridCursor<Void> cursor = t.sweep(clo);
        while (cursor.next())
            cursor.get();

        for (long k = 0L; k < NUM_KEYS; k++) {
            Long res = t.findOne(k);
            if (k%2 == 0)
                assertNull(res);
            else
                assertEquals(k, res.longValue());
        }

        t.destroy();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSweepConcurrentDeletes() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(0);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSweepConcurrentDeletesBackwards() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(1);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSweepConcurrentDeletesAll() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(2);
    }

    /**
     *
     * @param mode Mode.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestSweepConcurrentDeletes(int mode) throws IgniteCheckedException {
        final TestTree t = createTestTree(true);

        BPlusTree.TreeRowClosure<Long, Long> clo = (tree, io, pageAddr, idx) ->
            io.getLookupRow(tree, pageAddr, idx) % 2 == 0;

        for (long k = 0L; k < NUM_KEYS; k++)
            t.putx(k);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    switch (mode) {
                        case 0:
                            for (long k = 0L; k < NUM_KEYS; k++) {
                                if (k%2==1)
                                    t.removex(k);
                            }
                            break;
                        case 1:
                            for (long k = NUM_KEYS; k > 0; k--) {
                                if (k%2==1)
                                    t.removex(k);
                            }
                            break;
                        case 2:
                            for (long k = 0L; k < NUM_KEYS; k++)
                                t.removex(k);

                            break;
                    }

                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });

        thread.start();

        GridCursor<Void> cursor = t.sweep(clo);
        while (cursor.next())
            cursor.get();

        try {
            thread.join();
        }
        catch (InterruptedException e) {
            fail(e.getMessage());
        }

        assertTrue(t.isEmpty());
        t.destroy();
    }
}
