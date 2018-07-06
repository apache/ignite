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

package org.apache.ignite.internal.processors.cache.datastructures;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteMultimap;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.datastructures.GridCacheAbstractMapImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheMultimapImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheMultimapProxy;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotEquals;

/**
 * Multimap basic tests.
 */
public abstract class GridCacheMultimapApiSelfAbstractTest extends IgniteCollectionAbstractTest {
    /** */
    private static final int THREAD_NUM = 2;
    private static final String NON_EXISTING_KEY = "non-existing-key";
    private static final String NON_EXISTING_VALUE = "non-existing-value";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    protected GridCacheAdapter getMultimapBackingCache(IgniteMultimap multimap) throws Exception {
        Field field = GridCacheMultimapProxy.class
            .getDeclaredField("delegate");
        field.setAccessible(true);
        GridCacheMultimapImpl delegate = (GridCacheMultimapImpl)field.get(multimap);

        field = GridCacheAbstractMapImpl.class.getDeclaredField("cache");
        field.setAccessible(true);
        return (GridCacheAdapter)field.get(delegate);
    }

    /**
     * Multimap collocation mode
     *
     * @return true if multimap is collocated, false otherwise
     */
    protected abstract boolean collocated();

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrepareMultimap() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();

        CollectionConfiguration colCfg = config(collocated());

        IgniteMultimap multimap1 = grid(0).multimap(multimapName1, colCfg);
        IgniteMultimap multimap2 = grid(0).multimap(multimapName2, colCfg);
        IgniteMultimap multimap3 = grid(0).multimap(multimapName1, colCfg);

        assertNotNull(multimap1);
        assertNotNull(multimap2);
        assertNotNull(multimap3);
        assertEquals(multimap1, multimap3);
        assertEquals(multimap3, multimap1);
        assertNotEquals(multimap3, multimap2);

        multimap1.close();
        multimap2.close();

        assertNull(grid(0).multimap(multimapName1, null));
        assertNull(grid(0).multimap(multimapName2, null));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public abstract void testMultimapCollocationMode() throws Exception;

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(multimap.get(NON_EXISTING_KEY).isEmpty());

        multimap.put(key, val1);
        assertEquals(1, multimap.get(key).size());
        multimap.put(key, val2);
        assertEquals(2, multimap.get(key).size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetByIndex() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertNull(multimap.get(NON_EXISTING_KEY, 0));

        multimap.put(key, val1);
        assertEquals(val1, multimap.get(key, 0));
        multimap.put(key, val2);
        assertEquals(val2, multimap.get(key, 1));

        try {
            multimap.get(key, 5);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetByRangeIndex() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(multimap.get(NON_EXISTING_KEY, 0, 3).isEmpty());

        multimap.put(key, val1);
        multimap.put(key, val2);
        multimap.put(key, val3);
        assertEqualsCollections(asList(val2, val3), multimap.get(key, 1, 2));

        try {
            multimap.get(key, 1, 3);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetByIterable() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(multimap.get(NON_EXISTING_KEY, asList(1, 2)).isEmpty());

        multimap.put(key, val1);
        multimap.put(key, val2);
        multimap.put(key, val3);
        assertEqualsCollections(asList(val2, val3), multimap.get(key, asList(1, 2)));

        try {
            multimap.get(key, asList(1, 2, 3));
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key2, val2);

        Map<String, List<String>> all = multimap.getAll(asList(key1, key2, NON_EXISTING_KEY));

        assertEqualsCollections(Collections.singletonList(val1), all.get(key1));
        assertEqualsCollections(Collections.singletonList(val2), all.get(key2));
        assertTrue(all.get(NON_EXISTING_KEY).isEmpty());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetAllByIndex() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key1, val2);
        multimap.put(key2, val1);
        multimap.put(key2, val2);

        Map<String, String> map = multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), 1);

        assertEquals(val2, map.get(key1));
        assertEquals(val2, map.get(key2));
        assertNull(map.get(NON_EXISTING_KEY));

        try {
            multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), 5);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetAllByRangeIndex() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key1, val2);
        multimap.put(key2, val1);
        multimap.put(key2, val2);

        Map<String, List<String>> map = multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), 0, 1);

        assertEqualsCollections(asList(val1, val2), map.get(key1));
        assertEquals(asList(val1, val2), map.get(key2));
        assertTrue(map.get(NON_EXISTING_KEY).isEmpty());

        try {
            multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), 1, 5);
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testGetAllByIterable() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key1, val2);
        multimap.put(key2, val1);
        multimap.put(key2, val2);

        Map<String, List<String>> map = multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), asList(0, 1));

        assertEqualsCollections(asList(val1, val2), map.get(key1));
        assertEquals(asList(val1, val2), map.get(key2));
        assertTrue(map.get(NON_EXISTING_KEY).isEmpty());

        try {
            multimap.getAll(asList(key1, key2, NON_EXISTING_KEY), asList(1, 2, 5));
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testClear() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap1 = grid(0).multimap(multimapName1, config(collocated()));
        IgniteMultimap<String, String> multimap2 = grid(0).multimap(multimapName2, config(collocated()));

        multimap1.put(key1, val1);
        multimap1.put(key1, val2);
        multimap1.put(key2, val1);
        multimap1.put(key2, val2);
        multimap2.put(key1, val1);
        multimap2.put(key1, val2);

        assertEquals(2, multimap1.get(key1).size());
        assertEquals(2, multimap1.get(key2).size());
        assertEquals(2, multimap2.get(key1).size());

        multimap1.clear();

        List<String> list1 = multimap1.get(key1);
        assertTrue(list1.isEmpty());
        List<String> list2 = multimap1.get(key2);
        assertTrue(list2.isEmpty());
        assertEquals(2, multimap2.get(key1).size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testContainsKey() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key1, val2);

        assertTrue(multimap.containsKey(key1));
        assertFalse(multimap.containsKey(key2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testContainsValue() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap1 = grid(0).multimap(multimapName1, config(collocated()));
        IgniteMultimap<String, String> multimap2 = grid(0).multimap(multimapName2, config(collocated()));

        multimap1.put(key, val1);
        multimap2.put(key, val2);

        assertTrue(multimap1.containsValue(val1));
        assertFalse(multimap1.containsValue(val2));

        assertFalse(multimap2.containsValue(val1));
        assertTrue(multimap2.containsValue(val2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testContainsEntry() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap1 = grid(0).multimap(multimapName1, config(collocated()));
        IgniteMultimap<String, String> multimap2 = grid(0).multimap(multimapName2, config(collocated()));

        multimap1.put(key1, val1);
        multimap2.put(key2, val2);

        assertTrue(multimap1.containsEntry(key1, val1));
        assertFalse(multimap1.containsEntry(key1, val2));
        assertFalse(multimap1.containsEntry(key2, val2));
        assertTrue(multimap2.containsEntry(key2, val2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testEntries() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key1, Arrays.asList(val1, val2, val3));
        multimap.putAll(key2, Arrays.asList(val1, val2));

        Set<String> keys = new HashSet<>();
        List<String> values = new ArrayList<>();
        Iterator<Map.Entry<String, String>> it = multimap.entries();
        while (it.hasNext()) {
            Map.Entry<String, String> e = it.next();
            keys.add(e.getKey());
            values.add(e.getValue());
        }
        assertEquals(2, keys.size());
        assertTrue(keys.contains(key1));
        assertTrue(keys.contains(key2));

        assertEquals(5, values.size());
        assertTrue(values.contains(val1));
        assertTrue(values.contains(val2));
        assertTrue(values.contains(val3));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testLocalKeySet() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key2, val2);

        Set<String> set = multimap.localKeySet();

        assertEquals(2, set.size());
        assertTrue(set.contains(key1));
        assertTrue(set.contains(key2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testKeySet() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.put(key1, val1);
        multimap.put(key2, val2);

        Set<String> set = multimap.keySet();

        assertEquals(2, set.size());
        assertTrue(set.contains(key1));
        assertTrue(set.contains(key2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(multimap.put(key, val1));
        assertTrue(multimap.put(key, val2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutGetMultithreaded() throws Exception {
        String multimapName = UUID.randomUUID().toString();
        final IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        final String key = "key";
        multithreaded((Callable<Void>)() -> {
            String thName = Thread.currentThread().getName();
            for (int i = 0; i < 5; i++)
                multimap.put(key, thName + i);
            return null;
        }, THREAD_NUM);

        assertEquals(10, multimap.get(key).size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutAll() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertTrue(multimap.putAll(key, Arrays.asList(val1, val2)));
        assertEquals(2, multimap.get(key).size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutAllFromMultimap() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();
        String multimapName3 = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap1 = grid(0).multimap(multimapName1, config(collocated()));
        IgniteMultimap<String, String> multimap2 = grid(0).multimap(multimapName2, config(collocated()));
        IgniteMultimap<String, String> multimap3 = grid(0).multimap(multimapName3, config(collocated()));

        multimap1.putAll(key, Arrays.asList(val1, val2));

        assertTrue(multimap2.putAll(multimap1));
        assertEquals(1, multimap2.size());
        assertEquals(2, multimap2.get(key).size());

        assertFalse(multimap2.putAll(multimap3));
        assertEquals(1, multimap2.size());
        assertEquals(2, multimap2.get(key).size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key1, Arrays.asList(val1, val2));
        multimap.put(key2, val2);

        multimap.remove(key1);

        assertFalse(multimap.containsKey(key1));
        assertTrue(multimap.containsKey(key2));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testRemoveByKeyValue() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key, Arrays.asList(val1, val2));
        multimap.remove(key, val2);

        assertEquals(1, multimap.get(key).size());
        assertEquals(val1, multimap.get(key).get(0));
        assertFalse(multimap.remove(NON_EXISTING_KEY, val1));
        assertFalse(multimap.remove(key, NON_EXISTING_VALUE));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testReplaceValues() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();
        String val4 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key, Arrays.asList(val1, val2));
        assertTrue(multimap.replaceValues(NON_EXISTING_KEY, null).isEmpty());

        List<String> olsVals = multimap.replaceValues(key, Arrays.asList(val3, val4));
        assertEquals(2, olsVals.size());
        assertTrue(olsVals.contains(val1));
        assertTrue(olsVals.contains(val2));

        assertEquals(2, multimap.get(key).size());
        assertTrue(multimap.get(key).contains(val3));
        assertTrue(multimap.get(key).contains(val4));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testIterate() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key1, Arrays.asList(val1, val2, val3));
        multimap.putAll(key2, Arrays.asList(val1, val2));

        int idx = 0;
        Iterator<Map.Entry<String, String>> it = multimap.iterate(1);
        while (it.hasNext()) {
            it.next();
            idx++;
        }
        assertEquals(2, idx);

        idx = 0;
        it = multimap.iterate(2);
        while (it.hasNext()) {
            try {
                it.next();
                if (idx == 1)
                    fail();
                idx++;
            }
            catch (IndexOutOfBoundsException e) {
            }
        }
        assertEquals(1, idx);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSize() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key1, Arrays.asList(val1, val2));
        multimap.putAll(key2, Arrays.asList(val1, val2));
        assertEquals(2, multimap.size());

        multimap.remove(key1, val2);
        assertEquals(2, multimap.size());

        multimap.remove(key1);
        assertEquals(1, multimap.size());

        multimap.remove(key2);
        assertEquals(0, multimap.size());
        assertTrue(multimap.isEmpty());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testValues() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();
        String val3 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key1, Arrays.asList(val1, val2, val3));
        multimap.putAll(key2, Arrays.asList(val1, val2));

        List<String> values = new ArrayList<>();
        Iterator<String> it = multimap.values();
        while (it.hasNext())
            values.add(it.next());

        assertEquals(5, values.size());
        assertTrue(values.contains(val1));
        assertTrue(values.contains(val2));
        assertTrue(values.contains(val3));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testValueCount() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        String key = UUID.randomUUID().toString();
        String val1 = UUID.randomUUID().toString();
        String val2 = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        multimap.putAll(key, Arrays.asList(val1, val2));

        assertEquals(2, multimap.valueCount(key));
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testName() throws Exception {
        String multimapName = UUID.randomUUID().toString();

        IgniteMultimap<String, String> multimap = grid(0).multimap(multimapName, config(collocated()));

        assertEquals(multimapName, multimap.name());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testClose() throws Exception {
        String multimapName1 = UUID.randomUUID().toString();
        String multimapName2 = UUID.randomUUID().toString();

        CollectionConfiguration cfg = config(collocated());
        IgniteMultimap<String, String> multimap1 = grid(0).multimap(multimapName1, cfg);
        IgniteMultimap<String, String> multimap2 = grid(0).multimap(multimapName2, cfg);

        assertNotNull(multimap1);
        assertNotNull(multimap2);

        multimap1.close();

        assertTrue(multimap1.removed());
        assertFalse(multimap2.removed());

        multimap1 = grid(0).multimap(multimapName1, null);
        multimap2 = grid(0).multimap(multimapName2, null);

        assertNull(multimap1);
        assertNotNull(multimap2);
    }
}