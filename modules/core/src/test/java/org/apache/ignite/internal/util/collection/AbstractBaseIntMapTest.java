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

package org.apache.ignite.internal.util.collection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Common map tests.
 */
public abstract class AbstractBaseIntMapTest {
    /**
     * @return Returns particular implementation of IntMap.
     */
    protected abstract IntMap<String> instantiateMap();

    /**
     *
     */
    @Test
    public void sizeOfMap() {
        IntMap<String> map = instantiateMap();

        assertTrue(map.isEmpty());
        assertEquals(0, map.size());

        for (int i = 1; i < 100_000; i++) {
            map.put(i, value(i));
            assertFalse(map.isEmpty());
            assertEquals(i, map.size());
        }

        for (int i = 99_999; i > 0; i--) {
            map.remove(i);
            assertEquals(i - 1, map.size());
        }

        assertTrue(map.isEmpty());
    }

    /**
     *
     */
    @Test
    public void getEmpty() {
        IntMap<String> map = instantiateMap();

        assertNull(map.get(0));
    }

    /**
     *
     */
    @Test
    public void putAndGet() {
        IntMap<String> map = instantiateMap();

        map.put(0, value(0));
        map.put(239, value(239));
        map.put(677, value(677));

        assertEquals(value(0), map.get(0));
        assertEquals(value(239), map.get(239));
        assertEquals(value(677), map.get(677));
    }

    /**
     *
     */
    @Test
    public void getAbsentKey() {
        IntMap<String> map = instantiateMap();

        map.put(0, value(0));
        map.put(239, value(239));
        map.put(677, value(677));

        assertNull(map.get(32));
    }

    /**
     *
     */
    @Test
    public void putPresentKey() {
        IntMap<String> map = instantiateMap();

        map.put(0, value(0));
        String oldVal = map.put(0, value(1));

        assertEquals(value(0), oldVal);
        assertEquals(value(1), map.get(0));
        assertEquals(1, map.size());
    }

    /**
     *
     */
    @Test
    public void remove() {
        IntMap<String> map = instantiateMap();

        map.put(0, value(0));

        assertEquals(value(0), map.remove(0));
        assertEquals(0, map.size());
    }

    /**
     *
     */
    @Test
    public void removeAbsentKey() {
        IntMap<String> map = instantiateMap();

        assertNull(map.remove(0));
    }

    /**
     *
     */
    @Test
    public void putIfAbsent() {
        IntMap<String> map = instantiateMap();

        String newVal = map.putIfAbsent(1, value(1));

        assertNull(newVal);

        assertEquals(value(1), map.get(1));

        String retry = map.putIfAbsent(1, value(2));

        assertEquals(value(1), retry);
    }

    /**
     *
     */
    @Test
    public void forEach() {
        IntMap<String> map = instantiateMap();

        for (int i = 1; i < 100_000; i++)
            map.put(i, value(i));

        final AtomicLong cntr = new AtomicLong(0);

        map.forEach((key, value) -> cntr.addAndGet(key));

        assertEquals(99_999L * 100_000L / 2, cntr.get());
    }

    /**
     *
     */
    @Test
    public void contains() {
        IntMap<String> map = instantiateMap();

        for (int i = 1; i < 10_000; i++)
            map.put(i, value(i));

        for (int i = 1; i < 10_000; i++) {
            assertTrue(map.containsKey(i));
            assertTrue(map.containsValue(value(i)));
        }

        assertFalse(map.containsKey(0));
        assertFalse(map.containsValue(value(0)));
    }

    /**
     *
     */
    @Test
    public void compareWithReferenceImplementation() {
        Map<Integer, String> originMap = new HashMap<>();
        IntMap<String> testable = instantiateMap();

        ThreadLocalRandom randomGen = ThreadLocalRandom.current();

        for (int i = 0; i < 10_000_000; i++) {
            int nextKey = randomGen.nextInt(0, 1_000_000);
            int actId = randomGen.nextInt(0, 4);

            if (actId == 0) {
                String oPut = originMap.put(nextKey, value(nextKey));
                String ePut = testable.put(nextKey, value(nextKey));

                assertEquals(oPut, ePut);
                assertEquals(originMap.containsKey(nextKey), testable.containsKey(nextKey));
            }
            else if (actId == 1) {
                assertEquals(originMap.get(nextKey), testable.get(nextKey));
                assertEquals(originMap.containsKey(nextKey), testable.containsKey(nextKey));
            }
            else if (actId == 2) {
                String oPutAbs = originMap.putIfAbsent(nextKey, value(nextKey));
                String ePutAbs = testable.putIfAbsent(nextKey, value(nextKey));

                assertEquals(oPutAbs, ePutAbs);
                assertEquals(originMap.containsKey(nextKey), testable.containsKey(nextKey));
            }
            else {
                String oRmv = originMap.remove(nextKey);
                String eRmv = testable.remove(nextKey);

                assertEquals(oRmv, eRmv);
                assertEquals(originMap.get(nextKey), testable.get(nextKey));
                assertEquals(originMap.containsKey(nextKey), testable.containsKey(nextKey));
            }

            Assert.assertEquals(originMap.size(), testable.size());
        }
    }

    /**
     * @return Returns value by key.
     */
    protected String value(int key) {
        return "Value-" + key;
    }
}
