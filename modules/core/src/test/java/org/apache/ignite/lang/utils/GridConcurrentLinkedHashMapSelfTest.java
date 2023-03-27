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

import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q_OPTIMIZED_RMV;

/**
 * This class tests basic contracts of {@code ConcurrentLinkedHashMap}.
 */
public class GridConcurrentLinkedHashMapSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_UPPER_BOUND = 1000;

    /** */
    private static final int INSERTS_COUNT = 10000;

    /** Random used for key generation. */
    private static final Random rnd = new Random();

    /**
     *
     */
    @Test
    public void testInsertionOrder() {
        testOrder(false);
    }

    /**
     *
     */
    @Test
    public void testInsertionOrderWithUpdate() {
        testOrder(true);
    }

    /**
     *
     */
    @Test
    public void testEvictionInsert() {
        final int mapSize = 1000;

        Map<Integer, String> tst = new ConcurrentLinkedHashMap<>(10, 0.75f, 1, 1000);

        Map<Integer, String> map = new LinkedHashMap<Integer, String>(10, 0.75f, false) {
            @Override protected boolean removeEldestEntry(Map.Entry<Integer, String> eldest) {
                return size() > mapSize;
            }
        };

        for (int i = 0; i < mapSize; i++) {
            tst.put(i, "value" + i);
            map.put(i, "value" + i);
        }

        Random rnd1 = new Random();

        int iterCnt = 100000;
        int keyCnt = 10000;
        for (int i = 0; i < iterCnt; i++) {
            int key = rnd1.nextInt(keyCnt);

            tst.put(key, "value" + key);
            map.put(key, "value" + key);
        }

        Iterator<Map.Entry<Integer, String>> tstIt = tst.entrySet().iterator();

        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            assertTrue("No enough elements in key set", tstIt.hasNext());

            Map.Entry<Integer, String> tstEntry = tstIt.next();

            assertEquals("Key mismatch", tstEntry.getKey(), entry.getKey());
            assertEquals("Value mismatch", tstEntry.getValue(), entry.getValue());
        }

        assertEquals("Invalid map size", mapSize, tst.size());
    }

    /**
     * Tests order of elements in map.
     *
     * @param update {@code true} if test should update map via iterator.
     */
    private void testOrder(boolean update) {
        Map<Integer, String> tst = new ConcurrentLinkedHashMap<>();
        Map<Integer, String> map = new LinkedHashMap<>(10, 0.75f);

        for (int i = 0; i < INSERTS_COUNT; i++) {
            int key = rnd.nextInt(KEYS_UPPER_BOUND);

            if (rnd.nextBoolean()) {
                tst.put(key, "value" + key);
                map.put(key, "value" + key);
            }
            else {
                tst.remove(key);
                map.remove(key);
            }
        }

        if (update) {
            Iterator<Map.Entry<Integer, String>> tstIt = tst.entrySet().iterator();

            for (Iterator<Map.Entry<Integer, String>> mapIt = map.entrySet().iterator(); mapIt.hasNext(); ) {
                Map.Entry<Integer, String> entry = mapIt.next();
                assertTrue("No enough elements in key set", tstIt.hasNext());

                Map.Entry<Integer, String> tstEntry = tstIt.next();

                assertEquals("Key mismatch", tstEntry.getKey(), entry.getKey());
                assertEquals("Value mismatch", tstEntry.getValue(), entry.getValue());

                if (entry.getKey() % 2 == 0) {
                    mapIt.remove();
                    tstIt.remove();
                }
            }
        }

        Iterator<Map.Entry<Integer, String>> tstIt = tst.entrySet().iterator();

        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            assertTrue("No enough elements in key set after removal", tstIt.hasNext());

            Map.Entry<Integer, String> tstEntry = tstIt.next();

            assertEquals("Key mismatch after removal", tstEntry.getKey(), entry.getKey());
            assertEquals("Value mismatch after removal", tstEntry.getValue(), entry.getValue());
        }
    }

    /**
     * Tests iterator when concurrent modifications remove and add the same keys to the map.
     *
     */
    @Test
    public void testIteratorDuplicates() {
        Map<Integer, String> tst = new ConcurrentLinkedHashMap<>();

        for (int i = 0; i < 10; i++)
            tst.put(i, "val" + i);

        Iterator<Integer> it = tst.keySet().iterator();
        for (int i = 0; i < 5; i++) {
            assertTrue("Not enough elements", it.hasNext());
            assertEquals(i, it.next().intValue());
        }

        for (int i = 0; i < 5; i++) {
            tst.remove(i);
            tst.put(i, "val" + i);
        }

        for (int i = 5; i < 10; i++) {
            assertTrue("Not enough elements", it.hasNext());
            assertEquals(i, it.next().intValue());
        }

        assertFalse("Duplicate key", it.hasNext());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRehash() throws Exception {
        Map<Integer, Date> map = new ConcurrentLinkedHashMap<>(10);

        for (int i = 0; i < 100; i++)
            // Will initiate rehash in the middle.
            map.put(i, new Date(0));

        for (int i = 0; i < 100; i++)
            map.put(i, new Date(1));

        for (Date date : map.values())
            assertEquals(1L, date.getTime());
    }

    /**
     *
     */
    @Test
    public void testDescendingMethods() {
        ConcurrentLinkedHashMap<Integer, Integer> tst = new ConcurrentLinkedHashMap<>();

        for (int i = 0; i < 100; i++)
            tst.put(i, i);

        int nextVal = 99;

        for (int i : tst.descendingKeySet()) {
            assert nextVal == i : "Unexpected value: " + i;

            nextVal--;
        }

        assert nextVal == -1;

        // Test values.
        nextVal = 99;

        for (int i : tst.descendingValues()) {
            assert nextVal == i : "Unexpected value: " + i;

            nextVal--;
        }

        assert nextVal == -1 : "Unexpected value: " + nextVal;

        // Test entries.
        nextVal = 99;

        for (Map.Entry<Integer, Integer> e : tst.descendingEntrySet()) {
            assert nextVal == e.getKey() : "Unexpected value: " + nextVal;
            assert nextVal == e.getValue() : "Unexpected value: " + nextVal;

            nextVal--;
        }

        assert nextVal == -1 : "Unexpected value: " + nextVal;

        // Test keys as enumeration.
        nextVal = 99;

        for (Enumeration<Integer> e = tst.descendingKeys(); e.hasMoreElements();) {
            int i = e.nextElement();

            assert nextVal == i : "Unexpected value: " + i;

            nextVal--;
        }

        assert nextVal == -1 : "Unexpected value: " + nextVal;

        // Test keys as enumeration.
        nextVal = 99;

        for (Enumeration<Integer> e = tst.descendingElements(); e.hasMoreElements();) {
            int i = e.nextElement();

            assert nextVal == i : "Unexpected value: " + i;

            nextVal--;
        }

        assert nextVal == -1 : "Unexpected value: " + nextVal;
    }

    /**
     *
     */
    @Test
    public void testIterationInPerSegmentModes() {
        checkIteration(PER_SEGMENT_Q);
        checkIteration(PER_SEGMENT_Q_OPTIMIZED_RMV);
    }

    /**
     * @param plc Policy.
     */
    private void checkIteration(ConcurrentLinkedHashMap.QueuePolicy plc) {
        ConcurrentLinkedHashMap<Integer, Integer> map =
            new ConcurrentLinkedHashMap<>(10,
                0.75f,
                16,
                0,
                plc);

        Map<Integer, Integer> map0 = new HashMap<>();

        int cnt = 0;

        for (int i = 0; i < 100_000; i++) {
            int key = ThreadLocalRandom.current().nextInt(15000);
            int val = ThreadLocalRandom.current().nextInt(15000);

            Integer rmv0 = map0.put(key, val);

            if (rmv0 == null)
                cnt++;

            Integer rmv = map.put(key, val);

            assertEquals(rmv0, rmv);
        }

        int checkCnt = 0;

        for (Map.Entry<Integer, Integer> e : map.entrySet()) {
            checkCnt++;

            Integer rmv = map0.remove(e.getKey());

            assertNotNull(rmv);
            assertEquals(rmv, e.getValue());
        }

        assertEquals(cnt, checkCnt);

        info("Puts count: " + cnt);

        assert map0.isEmpty() : map0;
    }
}
