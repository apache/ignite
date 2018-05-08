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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
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

import static org.apache.ignite.configuration.DeploymentMode.SHARED;
import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_LOCKED;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;

/**
 * Tests that input keys are sorted before locking and sorting is working on different cache configurations.
 */
public abstract class CacheSortingKeysAbstractTest extends GridCacheAbstractSelfTest {
    /** */
    private IgniteCache<Object, Object> cache;

    /** Used for invoke tests. */
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
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME).withKeepBinary().withAutoSorting();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
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
        IgniteCache<String, Integer> cache = grid(0).createCache("deadlock").withAutoSorting();

        Map<String, Integer> m1 = new TreeMap<>();
        Map<String, Integer> m2 = new HashMap<>(U.capacity(10_000));

        for (int i = 0; i < 10_000; i++) {
            m1.put(i + "", i);
            m2.put(i + "", i + 1);
        }

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> cache.putAll(m1));

        cache.putAll(m2);

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllComparable() throws Exception {
        checkPutAll(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllNotcomparable() throws Exception {
        checkPutAll(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkPutAll(new P2PComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutAll(ComplexObject obj) throws Exception {
        SortingPredicate pred = createEventListener();

        cache.putAll(obj.unsortedMap());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllComparable() throws Exception {
        checkInvokeAll(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllNotcomparable() throws Exception {
        checkInvokeAll(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkInvokeAll(new P2PComplexObject(cache));
    }

    /**
     * @param obj ComplexObject.
     * @throws Exception If failed.
     */
    private void checkInvokeAll(ComplexObject obj) throws Exception {
        SortingPredicate pred = createEventListener();

        cache.invokeAll(obj.unsortedInvokeMap());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllComparable() throws Exception {
        checkRemoveAll(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllNotcomparable() throws Exception {
        checkRemoveAll(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkRemoveAll(new P2PComplexObject(cache));
    }

    /**
     * @param obj ComplexObject.
     * @throws Exception If failed.
     */
    private void checkRemoveAll(ComplexObject obj) throws Exception {
        Map<Object, Object> map = obj.unsortedMap();

        cache.putAll(map);

        SortingPredicate pred = createEventListener();

        cache.removeAll(map.keySet());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearAllComparable() throws Exception {
        checkClearAll(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearAllNotcomparable() throws Exception {
        checkClearAll(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClearAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkClearAll(new P2PComplexObject(cache));
    }

    /**
     * @param obj ComplexObject.
     * @throws Exception If failed.
     */
    private void checkClearAll(ComplexObject obj) throws Exception {
        Map<Object, Object> map = obj.unsortedMap();

        cache.putAll(map);

        SortingPredicate pred = createEventListener();

        cache.clearAll(map.keySet());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllComparable() throws Exception {
        checkGetAll(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllNotcomparable() throws Exception {
        checkGetAll(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAllP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkGetAll(new P2PComplexObject(cache));
    }

    /**
     * @param obj ComplexObject.
     * @throws Exception If failed.
     */
    private void checkGetAll(ComplexObject obj) throws Exception {
        Map<Object, Object> map = obj.unsortedMap();

        cache.putAll(map);

        SortingPredicate pred = createEventListener();

        cache.getAll(map.keySet());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetEntriesComparable() throws Exception {
        checkGetEntries(new ComparableComplexObject());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetEntriesNotcomparable() throws Exception {
        checkGetEntries(new NotComparableComplexObject(cache));
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetEntriesP2P() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-5038");

        checkGetEntries(new P2PComplexObject(cache));
    }

    /**
     * @param obj ComplexObject.
     * @throws Exception If failed.
     */
    private void checkGetEntries(ComplexObject obj) throws Exception {
        Map<Object, Object> map = obj.unsortedMap();

        cache.putAll(map);

        SortingPredicate pred = createEventListener();

        cache.getAll(map.keySet());

        removeEventListener(pred);

        assertTrue("Sorting failed.", obj.checkOrder(pred.list));
    }

    /**
     * We use several events because they differs in the same test for different cache configurations.
     *
     * @return SortingPredicate.
     */
    private SortingPredicate createEventListener() {
        SortingPredicate pred = new SortingPredicate();

        grid(0).events().localListen(pred,
            EVT_CACHE_OBJECT_PUT, EVT_CACHE_ENTRY_CREATED, EVT_CACHE_OBJECT_LOCKED, EVT_CACHE_OBJECT_READ);

        return pred;
    }

    /**
     * @param sp SortingPredicate to remove from node.
     */
    private void removeEventListener(SortingPredicate sp) {
        grid(0).events().stopLocalListen(sp);
    }

    /** */
    private final class SortingPredicate implements IgnitePredicate<CacheEvent> {
        /** */
        private ArrayList<Object> list = new ArrayList<>();

        /** We should check only the same type of events. */
        private int evtType = -1;

        /** {@inheritDoc} */
        @Override public synchronized boolean apply(CacheEvent event) {
            if (evtType == -1)
                evtType = event.type();

            if (event.type() == evtType)
                list.add(event.key());

            return true;
        }
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

    /** */
    private abstract class ComplexObject {
        /** Array of not comparable objects wrapped in {@link BinaryObject}. Sorted by BinaryObject's hashcode. */
        final Object binaryObjects[] = new Object[4];

        /** Array of not comparable objects. Sorted by hashcode. */
        protected final Object objects[] = new Object[4];

        /** Used to get values from binary objects. */
        protected BinaryField field;

        /**
         * @param list List of keys.
         * @return {@code True} if keys were sorted. Otherwise - {@code false}.
         */
        protected boolean checkOrder(List list) {
            int cntr = 0;

            for (Object obj : list) {
                boolean equalP2PObjects = obj.equals(objects[cntr]);
                boolean isBinary = obj instanceof BinaryObject;
                boolean equalP2PBinaries = isBinary && binaryObjects[cntr] != null &&
                    field.value((BinaryObject)obj).equals(field.value((BinaryObjectEx)binaryObjects[cntr]));

                if (equalP2PObjects || equalP2PBinaries) {
                    if (++cntr == objects.length)
                        break;
                }
            }

            return cntr >= objects.length - 1;
        }

        /**
         * @return Unsorted map.
         */
        private Map<Object, Object> unsortedMap() {
            LinkedHashMap<Object, Object> map = new LinkedHashMap<>(U.capacity(4));

            map.put(objects[3], objects[3]);
            map.put(objects[0], objects[0]);
            map.put(objects[2], objects[2]);
            map.put(objects[1], objects[1]);

            return map;
        }

        /**
         * @return Unsorted map.
         */
        private Map<Object, EntryProcessor<Object, Object, Object>> unsortedInvokeMap() {
            Map<Object, EntryProcessor<Object, Object, Object>> map = new LinkedHashMap<>();

            map.put(objects[3], proc);
            map.put(objects[0], proc);
            map.put(objects[2], proc);
            map.put(objects[1], proc);

            return map;
        }
    }

    /** */
    private final class ComparableComplexObject extends ComplexObject {
        /** */
        private ComparableComplexObject() {
            objects[0] = "1_first_entry";
            objects[1] = "2_second_entry";
            objects[2] = "3_third_entry";
            objects[3] = "4_fourth_entry";
        }

        /** {@inheritDoc} */
        @Override protected boolean checkOrder(List list) {
            int sortingCntr = 0;

            for (int i = 0; i < list.size() && sortingCntr < objects.length; i++) {
                if (list.get(i).equals(objects[sortingCntr]))
                    sortingCntr++;
            }

            return sortingCntr >= objects.length - 1;
        }
    }

    /** */
    private final class NotComparableComplexObject extends ComplexObject {
        /** {@inheritDoc} */
        private NotComparableComplexObject(IgniteCache cache) {
            for (int i = 0; i < objects.length; i++) {
                objects[i] = new NotComparable(i);

                binaryObjects[i] = ((IgniteCacheProxy)cache).context().kernalContext().cacheObjects().binary()
                    .toBinary(objects[i]);
            }

            Arrays.sort(binaryObjects, Comparator.comparingInt(Object::hashCode));

            field = ((BinaryObjectEx)binaryObjects[0]).rawType().field("val");
        }
    }

    /** */
    private final class P2PComplexObject extends ComplexObject {
        /**  */
        private static final String P2P_PERSON = "org.apache.ignite.tests.p2p.cache.Person";

        /**
         * @param cache IgniteCache to get IgniteBinary.
         */
        private P2PComplexObject(IgniteCache cache) {
            initializeObjects((IgniteCacheProxy)cache);

            Arrays.sort(objects, Comparator.comparingInt(Object::hashCode));
            Arrays.sort(binaryObjects, Comparator.comparingInt(Object::hashCode));

            field = ((BinaryObjectEx)binaryObjects[0]).rawType().field("id");
        }

        /**
         * Loads {@link #P2P_PERSON} class and creates object arrays with this class.
         *
         * @param cache IgniteCache to get IgniteBinary.
         */
        private void initializeObjects(IgniteCacheProxy cache) {
            try {
                ClassLoader ldr = getExternalClassLoader();

                Class personCls = ldr.loadClass(P2P_PERSON);
                Field f = U.findField(personCls, "id");

                for (int i = 0; i < objects.length; i++) {
                    objects[i] = personCls.newInstance();

                    f.set(objects[i], i);

                    binaryObjects[i] = cache.context().kernalContext().cacheObjects().binary()
                        .toBinary(objects[i]);
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
