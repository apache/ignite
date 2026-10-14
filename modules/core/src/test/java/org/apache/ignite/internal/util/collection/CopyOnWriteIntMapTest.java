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

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test for copy-on-write int map. Every write copies the map, so the sizes are kept small on purpose.
 */
public class CopyOnWriteIntMapTest {
    /** */
    @Test
    public void putGetRemove() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        assertTrue(map.isEmpty());
        assertNull(map.get(1));
        assertFalse(map.containsKey(1));

        assertNull(map.put(1, "one"));
        assertEquals("one", map.put(1, "uno"));
        assertNull(map.put(-2, "two"));

        assertEquals(2, map.size());
        assertEquals("uno", map.get(1));
        assertEquals("two", map.get(-2));
        assertTrue(map.containsKey(-2));
        assertTrue(map.containsValue("two"));
        assertFalse(map.containsValue("one"));

        assertEquals("uno", map.remove(1));
        assertNull(map.remove(1));

        assertEquals(1, map.size());
        assertFalse(map.containsKey(1));

        map.clear();

        assertTrue(map.isEmpty());
        assertNull(map.get(-2));
    }

    /** */
    @Test
    public void putIfAbsentAndComputeIfAbsent() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        assertNull(map.putIfAbsent(1, "one"));
        assertEquals("one", map.putIfAbsent(1, "uno"));
        assertEquals("one", map.get(1));

        assertEquals("one", map.computeIfAbsent(1, k -> "computed"));
        assertEquals("2", map.computeIfAbsent(2, String::valueOf));
        assertEquals("2", map.get(2));
    }

    /** */
    @Test
    public void compareWithReferenceImplementation() {
        CopyOnWriteIntMap<Integer> map = new CopyOnWriteIntMap<>();
        Map<Integer, Integer> ref = new HashMap<>();

        Random rnd = new Random(0);

        for (int i = 0; i < 10_000; i++) {
            int key = rnd.nextInt(500);

            switch (rnd.nextInt(3)) {
                case 0:
                    assertEquals(ref.put(key, i), map.put(key, i));

                    break;

                case 1:
                    assertEquals(ref.putIfAbsent(key, i), map.putIfAbsent(key, i));

                    break;

                default:
                    assertEquals(ref.remove(key), map.remove(key));
            }

            assertEquals(ref.get(key), map.get(key));
            assertEquals(ref.size(), map.size());
        }
    }

    /** */
    @Test
    public void valuesIsLiveView() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        Collection<String> vals = map.values();

        assertSame(vals, map.values());
        assertTrue(vals.isEmpty());

        map.put(1, "one");

        assertEquals(1, vals.size());
        assertTrue(vals.contains("one"));

        map.remove(1);

        assertTrue(vals.isEmpty());
    }

    /** */
    @Test
    public void iteratorSeesSnapshot() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        map.put(1, "one");

        Iterator<String> it = map.values().iterator();

        map.put(2, "two");
        map.remove(1);

        assertEquals("one", it.next());
        assertFalse(it.hasNext());
    }

    /** */
    @Test
    public void conditionalRemove() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        String val = "one";

        map.put(1, val);

        assertFalse(map.remove(1, "other"));
        assertEquals(val, map.get(1));

        assertTrue(map.remove(1, new String(val)));
        assertFalse(map.containsKey(1));
        assertFalse(map.remove(1, val));
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void throwExceptionForValuesModification() {
        CopyOnWriteIntMap<String> map = new CopyOnWriteIntMap<>();

        map.put(1, "one");

        map.values().iterator().remove();
    }
}
