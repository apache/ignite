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

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

/**
 * Tests for {@link GridLeanMap}.
 */
@GridCommonTest(group = "Lang")
public class GridLeanMapSelfTest extends GridCommonAbstractTest {
    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testDefaultMap() throws Exception {
        Map<String, String> map = new GridLeanMap<>();

        checkEmptyMap(map);
        checkImpl(map, "Map3");

        try {
            map.keySet().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.entrySet().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.values().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.keySet().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.entrySet().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.values().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    public void testEmptyMap() throws Exception {
        Map<String, String> map = new GridLeanMap<>(0);

        checkEmptyMap(map);
        checkImplBySize(map);

        try {
            map.keySet().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.entrySet().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.values().iterator().next();

            fail("NoSuchElementException must have been thrown.");
        }
        catch (NoSuchElementException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.keySet().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.entrySet().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }

        try {
            map.values().iterator().remove();

            fail("IllegalStateException must have been thrown.");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOneEntryMap() throws Exception {
        Map<String, String> map = new GridLeanMap<>(0);

        map.put("k1", "v1");

        checkImplBySize(map);

        assert !map.isEmpty();

        assertEquals(1, map.size());

        assert map.containsKey("k1");

        assert map.containsValue("v1");

        assert !map.keySet().isEmpty();

        assert !map.entrySet().isEmpty();

        assert !map.values().isEmpty();

        assertEquals("v1", map.get("k1"));

        assert map.get("k2") == null;

        Iterator<String> kIter = map.keySet().iterator();

        assertEquals("k1", kIter.next());

        assert !kIter.hasNext();

        Iterator<Map.Entry<String, String>> eIter = map.entrySet().iterator();

        Map.Entry<String, String> entry = eIter.next();

        assert entry != null;

        assertEquals("k1", entry.getKey());

        assertEquals("v1", entry.getValue());

        assert !eIter.hasNext();

        Iterator<String> vIter = map.values().iterator();

        assertEquals("v1", vIter.next());

        assert !vIter.hasNext();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapPutSameKey() throws Exception {
        Map<String, String> map = new GridLeanMap<>(0);

        map.put("k1", "v1");
        map.put("k1", "v2");

        checkImplBySize(map);

        assertEquals(1, map.size());

        assertEquals("v2", map.get("k1"));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMultipleEntriesMap() throws Exception {
        Map<String, String> map = new GridLeanMap<>(0);

        map.put("k1", "v1");

        checkImplBySize(map);

        map.put("k2", "v2");

        checkImplBySize(map);

        map.put("k3", "v3");

        checkImplBySize(map);

        map.put("k4", "v4");

        checkImplBySize(map);

        map.put("k5", "v5");

        checkImplBySize(map);

        map.put("k6", "v6");

        assert !map.isEmpty();

        assertEquals(6, map.size());

        assert map.containsKey("k1");
        assert map.containsKey("k2");
        assert map.containsKey("k3");
        assert map.containsKey("k4");
        assert map.containsKey("k5");
        assert map.containsKey("k6");

        assert map.containsValue("v1");
        assert map.containsValue("v2");
        assert map.containsValue("v3");
        assert map.containsValue("v4");
        assert map.containsValue("v5");
        assert map.containsValue("v6");

        assert !map.keySet().isEmpty();

        assert !map.entrySet().isEmpty();

        assert !map.values().isEmpty();

        assertEquals("v1", map.get("k1"));
        assertEquals("v2", map.get("k2"));
        assertEquals("v3", map.get("k3"));
        assertEquals("v4", map.get("k4"));
        assertEquals("v5", map.get("k5"));
        assertEquals("v6", map.get("k6"));

        assert map.get("k7") == null;

        Iterator<String> kIter = map.keySet().iterator();

        for (int i = 0; i < 6; i++)
            assert kIter.next().contains("k");

        assert !kIter.hasNext();

        Iterator<Map.Entry<String, String>> eIter = map.entrySet().iterator();

        for (int i = 0; i < 6; i++) {
            Map.Entry<String, String> entry = eIter.next();

            assert entry != null : "Empty entry number: " + i;

            assert entry.getKey().contains("k") : "Invalid entry number: " + i;
            assert entry.getValue().contains("v") : "Invalid entry number: " + i;
        }

        assert !eIter.hasNext();

        Iterator<String> vIter = map.values().iterator();

        for (int i = 0; i < 6; i++)
            assert vIter.next().contains("v");

        assert !vIter.hasNext();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapRemove() throws Exception {
        Map<String, String> map = new GridLeanMap<>(0);

        // Put 1 entry.
        map.put("k1", "v1");

        checkImpl(map, "Map1");

        assertEquals("v1", map.remove("k1"));

        checkEmptyMap(map);
        checkImpl(map, null);

        // Put 2 entries.
        map.put("k1", "v1");
        map.put("k2", "v2");

        checkImpl(map, "Map2");

        assertEquals("v1", map.remove("k1"));
        assertEquals("v2", map.remove("k2"));

        checkEmptyMap(map);
        checkImpl(map, null);

        // Put more than 5 entries.
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        map.put("k4", "v4");
        map.put("k5", "v5");
        map.put("k6", "v6");
        map.put("k7", "v7");

        checkImpl(map, "LeanHashMap");
        checkImplBySize(map);

        assertEquals("v1", map.remove("k1"));
        assertEquals("v2", map.remove("k2"));
        assertEquals("v3", map.remove("k3"));
        assertEquals("v4", map.remove("k4"));
        assertEquals("v5", map.remove("k5"));
        assertEquals("v6", map.remove("k6"));
        assertEquals("v7", map.remove("k7"));

        checkEmptyMap(map);
        checkImpl(map, null);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testMapClear() throws Exception {
        Map<String, String> map = new GridLeanMap<>();

        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        map.put("k4", "v4");
        map.put("k5", "v5");
        map.put("k6", "v6");

        map.clear();

        checkEmptyMap(map);
        checkImplBySize(map);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEntrySet() throws Exception {
        Map<String, String> map = new GridLeanMap<>();

        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        map.put("k4", "v4");
        map.put("k5", "v5");
        map.put("k6", "v6");

        checkImpl(map, "LeanHashMap");

        Iterator<Map.Entry<String, String>> iter = map.entrySet().iterator();

        assert iter.hasNext();

        Map.Entry<String, String> e = iter.next();

        assert e.getKey().contains("k");
        assert e.getValue().contains("v");

        iter.next();

        iter.remove();

        checkImpl(map, "Map5");

        assertEquals(5, map.size());

        assert iter.next() != null;
        assert iter.next() != null;

        iter.remove();

        checkImpl(map, "Map4");

        assertEquals(4, map.size());

        assert iter.next() != null;

        assert iter.hasNext();

        assert iter.next() != null;

        assert !iter.hasNext();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testWithInitSize1() throws Exception {
        // Batch mode.
        Map<String, String> map = new GridLeanMap<>(4);

        checkEmptyMap(map);
        checkImpl(map, "Map4");

        map.put("k1", "v1");

        assertEquals(1, map.size());
        checkImpl(map, "Map4");

        map.put("k2", "v2");

        assertEquals(2, map.size());
        checkImpl(map, "Map4");

        // The same key does not lead to implementation change.
        map.put("k2", "v2");

        assertEquals(2, map.size());
        checkImpl(map, "Map4");

        map.put("k3", "v3");

        assertEquals(3, map.size());
        checkImpl(map, "Map4");

        map.put("k4", "v4");

        assertEquals(4, map.size());
        checkImpl(map, "Map4");

        map.put("k5", "v5");

        assertEquals(5, map.size());
        checkImpl(map, "Map5");

        map.put("k6", "v6");

        assertEquals(6, map.size());
        checkImpl(map, "LeanHashMap");

        map = new GridLeanMap<>(4);

        checkEmptyMap(map);
        checkImpl(map, "Map4");

        map.put("k1", "v1");

        assertEquals(1, map.size());
        checkImpl(map, "Map4");

        map.put("k2", "v2");

        assertEquals(2, map.size());
        checkImpl(map, "Map4");

        // Any remove operation leads to map size optimization.
        assertEquals("v1", map.remove("k1"));
        checkImpl(map, "Map1");
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testWithInitSize2() throws Exception {
        // Batch mode.
        Map<String, String> map = new GridLeanMap<>(10);

        checkEmptyMap(map);
        checkImpl(map, "LeanHashMap");

        map.put("k1", "v1");

        assertEquals(1, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k2", "v2");

        assertEquals(2, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k3", "v3");

        assertEquals(3, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k4", "v4");

        assertEquals(4, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k5", "v5");

        assertEquals(5, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k6", "v6");

        assertEquals(6, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k7", "v7");

        assertEquals(7, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k8", "v8");

        assertEquals(8, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k9", "v9");

        assertEquals(9, map.size());
        checkImpl(map, "LeanHashMap");

        map.put("k10", "v10");

        assertEquals(10, map.size());
        checkImpl(map, "LeanHashMap");
    }

    /**
     * Checks map emptiness.
     *
     * @param map Map to check.
     * @throws Exception If failed.
     */
    private void checkEmptyMap(Map<?, ?> map) throws Exception {
        assert map.isEmpty();

        assert !map.containsKey("key");

        assert !map.containsValue("value");

        assertEquals(0, map.size());

        assert map.keySet().isEmpty();

        assert map.entrySet().isEmpty();

        assert map.values().isEmpty();

        assert !map.keySet().iterator().hasNext();

        assert !map.entrySet().iterator().hasNext();

        assert !map.values().iterator().hasNext();
    }

    /**
     * @param map Lean map.
     * @param implName Internal implementation simple class name.
     *      {@code null} for empty implementation.
     * @throws Exception If failed.
     */
    private void checkImpl(Map map, @Nullable String implName) throws Exception {
        assert map != null;

        Field f = GridLeanMap.class.getDeclaredField("map");

        f.setAccessible(true);

        Map impl = (Map)f.get(map);

        if (implName != null)
            assertEquals(implName, impl.getClass().getSimpleName());
        else
            assert impl == null;
    }

    /**
     * @param map Lean map.
     * @throws Exception If failed.
     */
    private void checkImplBySize(Map map) throws Exception {
        assert map != null;

        if (map.isEmpty())
            checkImpl(map, null);
        else if (map.size() == 1)
            checkImpl(map, "Map1");
        else if (map.size() == 2)
            checkImpl(map, "Map2");
        else if (map.size() == 3)
            checkImpl(map, "Map3");
        else if (map.size() == 4)
            checkImpl(map, "Map4");
        else if (map.size() == 5)
            checkImpl(map, "Map5");
        else
            checkImpl(map, "LeanHashMap");
    }
}