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

package org.apache.ignite.internal.processors.cache;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import javax.cache.processor.EntryProcessor;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests that input keys are sorted before locking and sorting is working on different cache configurations.
 */
public abstract class CacheSortingKeysAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private static volatile boolean isSorted = false;

    /** */
    protected static final Object mux = new Object();

    /** */
    private static int sortingCntr = 0;

    /** */
    private void resetCounter() {
        isSorted = false;

        synchronized (mux) {
            sortingCntr = 0;
        }
    }

    /** */
    private IgniteCache<Object, Object> cache;

    /** Sorted array of comparable objects. */
    private final String strings[] = {"1_first_entry", "2_second_entry", "3_third_entry", "4_fourth_entry"};

    /** Array of not comparable objects. It is sorted as {@link #binaryObjects}. */
    private final Object objectsAsBinary[] = {new NotComparable(1), new NotComparable(2),
        new NotComparable(3), new NotComparable(4)};

    /** Array of not comparable objects wrapped in {@link BinaryObject}. Sorted by BinaryObject's hashcode. */
    private final Object binaryObjects[] = new Object[4];

    /** Array of not comparable objects. Sorted by hashcode. */
    private final Object objects[] = new Object[4];

    /** Array of objects loaded by external classloader. It is sorted as {@link #binaryP2PObjects}. */
    private final Object p2pObjectsAsBinary[] = new Object[4];

    /**
     * Array of objects loaded by external classloader and wrapped in {@link BinaryObject}.
     * Sorted by BinaryObject's hashcode.
     */
    private final Object binaryP2PObjects[] = new Object[4];

    /** Array of objects loaded by external classloader. It is sorted by hashcode. */
    private final Object p2pObjects[] = new Object[4];

    /**  */
    private static final String P2P_PERSON = "org.apache.ignite.tests.p2p.cache.Person";

    /** Used to get values from binary objects. */
    private BinaryField field;

    /** Used for invoke tests.*/
    private final EntryProcessor<Object, Object, Object> proc = (entry, objects) -> {
        entry.setValue(entry.getKey());

        return entry.getKey();
    };

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1_000;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        grid(0).events().localListen(new IgnitePredicate<CacheEvent>() {
            @Override public boolean apply(CacheEvent event) {
                if (sortingCntr > 3)
                    return true;

                synchronized (mux) {
                    boolean equalStrings = event.key().equals(strings[sortingCntr]);
                    boolean equalObjects = event.key().equals(objects[sortingCntr]);
                    boolean equalP2PObjects = event.key().equals(p2pObjects[sortingCntr]);
                    boolean isBinary = event.key() instanceof BinaryObject;
                    boolean equalBinaries = isBinary && binaryObjects[0] != null &&
                        field.value(event.key()).equals(field.value((BinaryObjectEx)binaryObjects[sortingCntr]));
                    boolean equalP2PBinaries = isBinary && binaryP2PObjects[0] != null &&
                        field.value(event.key()).equals(field.value((BinaryObjectEx)binaryP2PObjects[sortingCntr]));

                    if (equalStrings || equalObjects || equalP2PObjects || equalBinaries || equalP2PBinaries)
                        sortingCntr++;

                    if (sortingCntr == 4)
                        isSorted = true;
                }

                return true;
            }
        }, EVT_CACHE_OBJECT_PUT, EVT_CACHE_ENTRY_CREATED, EVT_CACHE_OBJECT_LOCKED);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        resetCounter();

        cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary();
    }

    /** Sorts not comparable objects by hashcode. */
    private void sortBinaryObjects() {
        for (int i = 0; i < objectsAsBinary.length; i++) {
            objects[i] = objectsAsBinary[i];

            binaryObjects[i] = ((IgniteCacheProxy)cache).context().kernalContext().cacheObjects().binary()
                .toBinary(objectsAsBinary[i]);
        }

        Arrays.sort(objects, Comparator.comparingInt(Object::hashCode));

        for (int i = 0; i < binaryObjects.length; i++) {
            for (int j = i + 1; j < binaryObjects.length; j++) {
                if (binaryObjects[j].hashCode() < binaryObjects[i].hashCode()) {
                    Object o = objectsAsBinary[i];
                    objectsAsBinary[i] = objectsAsBinary[j];
                    objectsAsBinary[j] = o;

                    o = binaryObjects[i];
                    binaryObjects[i] = binaryObjects[j];
                    binaryObjects[j] = o;
                }
            }
        }

        field = ((BinaryObjectEx)binaryObjects[0]).rawType().field("val");
    }

    /** Sorts objects loaded by externall classloader. Sorts by hashcode. */
    private void sortP2PObjects() throws Exception {
        ClassLoader ldr = getExternalClassLoader();

        Class personCls = ldr.loadClass(P2P_PERSON);
        Field f = U.findField(personCls, "id");

        for (int i = 0; i < p2pObjectsAsBinary.length; i++) {
            p2pObjectsAsBinary[i] = personCls.newInstance();
            f.set(p2pObjectsAsBinary[i], i);

            p2pObjects[i] = p2pObjectsAsBinary[i];

            binaryP2PObjects[i] = ((IgniteCacheProxy)cache).context().kernalContext().cacheObjects().binary()
                .toBinary(p2pObjectsAsBinary[i]);
        }


        Arrays.sort(p2pObjects, Comparator.comparingInt(Object::hashCode));

        for (int i = 0; i < binaryP2PObjects.length; i++) {
            for (int j = i + 1; j < binaryP2PObjects.length; j++) {
                if (binaryP2PObjects[j].hashCode() < binaryP2PObjects[i].hashCode()) {
                    Object o = p2pObjectsAsBinary[i];
                    p2pObjectsAsBinary[i] = p2pObjectsAsBinary[j];
                    p2pObjectsAsBinary[j] = o;

                    o = binaryP2PObjects[i];
                    binaryP2PObjects[i] = binaryP2PObjects[j];
                    binaryP2PObjects[j] = o;
                }
            }
        }

        field = ((BinaryObjectEx)binaryP2PObjects[0]).rawType().field("id");
    }

    /**
     * @return Unsorted map with comparable objects.
     */
    private Map<Object, Object> comparableMap() {
        return getUnsortedMap(strings);
    }

    /**
     * @return Unsorted map with not comparable objects.
     */
    private Map<Object, Object> notComparableMap() {
        sortBinaryObjects();

        return getUnsortedMap(binaryObjects);
    }

    /**
     * @return Unsorted map with objects loaded by externall classloader.
     */
    private Map<Object, Object> p2pMap() throws Exception {
        sortP2PObjects();

        return getUnsortedMap(p2pObjectsAsBinary);
    }

    /**
     * @param arr Array of objects to fill the map.
     * @return Unsorted map with given objects.
     */
    private Map<Object, Object> getUnsortedMap(Object[] arr) {
        Map<Object, Object> map = new HashMap<>();

        map.put(arr[3], arr[3]);
        map.put(arr[0], arr[0]);
        map.put(arr[2], arr[2]);
        map.put(arr[1], arr[1]);

        return map;
    }

    /**
     * @return Map with unsorted comparable keys and EntryProcessor as value.
     */
    private Map<Object, EntryProcessor<Object, Object, Object>> comparableInvokeMap() {
        return getInvokeMap(strings);
    }

    /**
     * @return Map with unsorted not comparable keys and EntryProcessor as value.
     */
    private Map<Object, EntryProcessor<Object, Object, Object>> notComparableInvokeMap() {
        sortBinaryObjects();

        return getInvokeMap(binaryObjects);
    }

    /**
     * @return Map with unsorted keys loaded by externall classloader and EntryProcessor as value.
     */
    private Map<Object, EntryProcessor<Object, Object, Object>> getP2PInvokeMap() throws Exception {
        sortP2PObjects();

        return getInvokeMap(p2pObjectsAsBinary);
    }

    /** Map with unsorted comparable keys and EntryProcessor as value. */
    private Map<Object, EntryProcessor<Object, Object, Object>> p2pInvokeMap() throws Exception {
        return getP2PInvokeMap();
    }

    /** Map with unsorted keys and EntryProcessor as value. */
    private Map<Object, EntryProcessor<Object, Object, Object>> getInvokeMap(Object[] arr) {
        Map<Object, EntryProcessor<Object, Object, Object>> map = new HashMap<>();

        map.put(arr[3], proc);
        map.put(arr[0], proc);
        map.put(arr[2], proc);
        map.put(arr[1], proc);

        return map;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertTrue("Map wasn't sorted.", isSorted);

        cache.clear();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);
        cfg.setDeploymentMode(SHARED);

        return cfg;
    }

    /**
     * Checks deadlock during two parallel {@code putAll()} calls.
     *
     * @throws Exception If failed.
     */
    public void testPutAllPutAllDeadlock() throws Exception {
        IgniteCache<String, Integer> cache = grid(0).createCache("deadlock");
        Map<String, Integer> m1 = new TreeMap<>();
        Map<String, Integer> m2 = new HashMap<>();

        for (int i = 0; i < 10_000; i++) {
            m1.put(i+"", i);
            m2.put(i+"", i+1);
        }

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            cache.putAll(m1);
        });

        cache.putAll(m2);

        fut.get();

        isSorted = true;
    }

    /** */
    public void testPutAllComparable() throws Exception {
        checkPutAll(comparableMap());
    }

    /** */
    public void testPutAllNotcomparable() throws Exception {
        checkPutAll(notComparableMap());
    }

    /** */
    public void testPutAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkPutAll(p2pMap());
    }

    /** */
    private void checkPutAll(Map<Object, Object> map) throws Exception {
        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                cache.putAll(map);
            }
        } else
            cache.putAll(map);
    }

    /** */
    public void testPutAllAsyncComparable() throws Exception {
        checkPutAllAsync(comparableMap());
    }

    /** */
    public void testPutAllAsyncNotcomparable() throws Exception {
        checkPutAllAsync(notComparableMap());
    }

    /** */
    public void testPutAllAsyncP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkPutAllAsync(p2pMap());
    }

    /** */
    private void checkPutAllAsync(Map<Object, Object> map) throws Exception {
        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                cache.putAllAsync(map).get(1_000);
            }
        } else
            cache.putAllAsync(map).get(1_000);
    }

    /** */
    public void testInvokeAllComparable() throws Exception {
        checkInvokeAll(comparableInvokeMap());
    }

    /** */
    public void testInvokeAllNotcomparable() throws Exception {
        checkInvokeAll(notComparableInvokeMap());
    }

    /** */
    public void testInvokeAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkInvokeAll(p2pInvokeMap());
    }

    /** */
    private void checkInvokeAll(Map<Object, EntryProcessor<Object, Object, Object>> map) throws Exception {
        if (atomicityMode() == TRANSACTIONAL) {
            try (Transaction tx = grid(0).transactions().txStart()) {
                cache.invokeAll(map);
            }
        } else
            cache.invokeAll(map);
    }

    /** */
    public void testInvokeAllAsyncMapComparable() throws Exception {
        checkInvokeAllAsyncMap(comparableInvokeMap());
    }

    /** */
    public void testInvokeAllAsyncMapNotcomparable() throws Exception {
        checkInvokeAllAsyncMap(notComparableInvokeMap());
    }

    /** */
    public void testInvokeAllAsyncP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkInvokeAllAsyncMap(p2pInvokeMap());
    }

    /** */
    private void checkInvokeAllAsyncMap(Map<Object, EntryProcessor<Object, Object, Object>> map) throws Exception {
        cache.invokeAllAsync(map).get(1_000);
    }

    /** */
    public void testInvokeAllAsyncSetComparable() throws Exception {
        checkInvokeAllAsyncSet(comparableMap());
    }

    /** */
    public void testInvokeAllAsyncSetNotcomparable() throws Exception {
        checkInvokeAllAsyncSet(notComparableMap());
    }

    /** */
    public void testInvokeAllAsyncSetP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkInvokeAllAsyncSet(p2pMap());
    }

    /** */
    private void checkInvokeAllAsyncSet(Map<Object, Object> map) throws Exception {
        cache.invokeAllAsync(map.keySet(), proc).get(1_000);
    }

    /** */
    public void testRemoveAllComparable() throws Exception {
        checkRemoveAll(comparableMap());
    }

    /** */
    public void testRemoveAllNotcomparable() throws Exception {
        checkRemoveAll(notComparableMap());
    }

    /** */
    public void testRemoveAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkRemoveAll(p2pMap());
    }

    /** */
    private void checkRemoveAll(Map<Object, Object> map) throws Exception {
        cache.putAll(map);

        resetCounter();

        cache.removeAll(map.keySet());
    }

    /** */
    public void testRemoveAllAsyncComparable() throws Exception {
        checkRemoveAllAsync(comparableMap());
    }

    /** */
    public void testRemoveAllAsyncNotcomparable() throws Exception {
        checkRemoveAllAsync(notComparableMap());
    }

    /** */
    public void testRemoveAllAsyncP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkRemoveAllAsync(p2pMap());
    }

    /** */
    private void checkRemoveAllAsync(Map<Object, Object> map) throws Exception {
        cache.putAll(map);

        resetCounter();

        cache.removeAllAsync(map.keySet()).get(1_000);
    }

    /** */
    private static final class NotComparable {
        /** Value for hashcode to compare objects. */
        private final int val;

        /** {@inheritDoc} */
        private NotComparable(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            NotComparable that = (NotComparable)o;

            return val == that.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }
}
