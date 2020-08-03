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

package org.apache.ignite.internal.binary;

import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.cache.Cache.Entry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Tests for TreeMap and TreeSet structures.
 */
public class BinaryTreeSelfTest extends GridCommonAbstractTest {
    /** Data structure size. */
    private static final int SIZE = 100;

    /** Node name: server. */
    private static final String NODE_SRV = "srv";

    /** Node name: client. */
    private static final String NODE_CLI = "cli";

    /** Key to be used for cache operations. */
    private static final int KEY = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        Ignition.start(configuration(NODE_SRV, false));
        Ignition.start(configuration(NODE_CLI, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stop(NODE_CLI, true);
        G.stop(NODE_SRV, true);
    }

    /**
     * Test {@code TreeMap} data structure.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsValueRegularNoComparator() throws Exception {
        checkTreeMapAsValue(false, false);
    }

    /**
     * Test {@code TreeMap} data structure with comparator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsValueRegularComparator() throws Exception {
        checkTreeMapAsValue(false, true);
    }

    /**
     * Test {@code TreeMap} data structure with binary mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsValueBinaryNoComparator() throws Exception {
        checkTreeMapAsValue(true, false);
    }

    /**
     * Test {@code TreeMap} data structure with binary mode and comparator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsValueBinaryComparator() throws Exception {
        checkTreeMapAsValue(true, true);
    }

    /**
     * Test {@code TreeMap} data structure when used as key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsKeyNoComparator() throws Exception {
        checkTreeMapAsKey(false);
    }

    /**
     * Test {@code TreeMap} data structure with comparator when used as key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeMapAsKeyComparator() throws Exception {
        checkTreeMapAsKey(true);
    }

    /**
     * Check {@code TreeMap} data structure.
     *
     * @param useBinary Whether to go through binary mode.
     * @param useComparator Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeMapAsValue(boolean useBinary, boolean useComparator) throws Exception {
        // Populate map.
        TreeMap<TestKey, Integer> map;

        map = testMap(useComparator);

        // Put and get value from cache.
        cache().put(KEY, map);

        TreeMap<TestKey, Integer> resMap;

        if (useBinary) {
            BinaryObject resMapBinary = (BinaryObject)cache().withKeepBinary().get(KEY);

            resMap = resMapBinary.deserialize();
        }
        else
            resMap = (TreeMap<TestKey, Integer>)cache().get(KEY);

        // Ensure content is correct.
        if (useComparator)
            assert resMap.comparator() instanceof TestKeyComparator;
        else
            assertNull(resMap.comparator());

        assertEquals(map, resMap);

        cache().clear();
    }

    /**
     * Check {@code TreeMap} data structure when used as key.
     *
     * @param useComparator Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeMapAsKey(boolean useComparator) throws Exception {
        // Populate map.
        TreeMap<TestKey, Integer> map;

        map = testMap(useComparator);

        // Put and get value from cache.
        cache().put(map, KEY);

        TreeMap<TestKey, Integer> resMap = (TreeMap<TestKey, Integer>)((Entry)cache().iterator().next()).getKey();

        // Ensure content is correct.
        if (useComparator)
            assert resMap.comparator() instanceof TestKeyComparator;
        else
            assertNull(resMap.comparator());

        assertEquals(map, resMap);

        // Ensure value is correct.
        Integer resSameMap = (Integer)cache().get(map);

        assertEquals((Object)KEY, resSameMap);

        Integer resIdenticalMap = (Integer)cache().get(testMap(useComparator));

        assertEquals((Object)KEY, resIdenticalMap);

        // Ensure wrong comparator is not accepted.
        Integer resDifferentComp = (Integer)cache().get(testMap(!useComparator));

        assertEquals(null, resDifferentComp);

        cache().clear();
    }

    /** */
    private TreeMap<TestKey, Integer> testMap(boolean useComp) {
        TreeMap<TestKey, Integer> map;

        if (useComp) {
            map = new TreeMap<>(new TestKeyComparator());

            for (int i = 0; i < SIZE; i++)
                map.put(key(false, i), i);
        }
        else {
            map = new TreeMap<>();

            for (int i = 0; i < SIZE; i++)
                map.put(key(true, i), i);
        }

        return map;
    }

    /**
     * Test {@code TreeSet} data structure.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsValueRegularNoComparator() throws Exception {
        checkTreeSetAsValue(false, false);
    }

    /**
     * Test {@code TreeSet} data structure with comparator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsValueRegularComparator() throws Exception {
        checkTreeSetAsValue(false, true);
    }

    /**
     * Test {@code TreeSet} data structure with binary mode.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsValueBinaryNoComparator() throws Exception {
        checkTreeSetAsValue(true, false);
    }

    /**
     * Test {@code TreeSet} data structure with binary mode and comparator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsValueBinaryComparator() throws Exception {
        checkTreeSetAsValue(true, true);
    }

    /**
     * Test {@code TreeSet} data structure.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsKeyNoComparator() throws Exception {
        checkTreeSetAsKey(false);
    }

    /**
     * Test {@code TreeSet} data structure with comparator.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTreeSetAsKeyComparator() throws Exception {
        checkTreeSetAsKey(true);
    }

    /**
     * Check {@code TreeSet} data structure.
     *
     * @param useBinary Whether to go through binary mode.
     * @param useComparator Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeSetAsValue(boolean useBinary, boolean useComparator) throws Exception {
        // Populate set.
        TreeSet<TestKey> set;

        set = testSet(useComparator);

        // Put and get value from cache.
        cache().put(KEY, set);

        TreeSet<TestKey> resSet;

        if (useBinary) {
            BinaryObject resMapBinary = (BinaryObject)cache().withKeepBinary().get(KEY);

            resSet = resMapBinary.deserialize();
        }
        else
            resSet = (TreeSet<TestKey>)cache().get(KEY);

        // Ensure content is correct.
        if (useComparator)
            assert resSet.comparator() instanceof TestKeyComparator;
        else
            assertNull(resSet.comparator());

        assertEquals(set, resSet);

        cache().clear();
    }

    /**
     * Check {@code TreeSet} data structure when used as key.
     *
     * @param useComp Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeSetAsKey(boolean useComp) throws Exception {
        // Populate set.
        TreeSet<TestKey> set;

        set = testSet(useComp);

        // Put and get value from cache.
        cache().put(set, KEY);

        TreeSet<TestKey> resSet = (TreeSet<TestKey>)((Entry)cache().iterator().next()).getKey();

        // Ensure content is correct.
        if (useComp)
            assert resSet.comparator() instanceof TestKeyComparator;
        else
            assertNull(resSet.comparator());

        assertEquals(set, resSet);

        // Ensure value is correct.
        Integer resSameMap = (Integer)cache().get(set);

        assertEquals((Object)KEY, resSameMap);

        Integer resIdenticalMap = (Integer)cache().get(testSet(useComp));

        assertEquals((Object)KEY, resIdenticalMap);

        // Ensure wrong comparator is not accepted.
        Integer resDifferentComp = (Integer)cache().get(testSet(!useComp));

        assertEquals(null, resDifferentComp);

        assertEquals(set, resSet);

        cache().clear();
    }

    /** */
    private TreeSet<TestKey> testSet(boolean useComp) {
        TreeSet<TestKey> set;

        if (useComp) {
            set = new TreeSet<>(new TestKeyComparator());

            for (int i = 0; i < SIZE; i++)
                set.add(key(false, i));
        }
        else {
            set = new TreeSet<>();

            for (int i = 0; i < SIZE; i++)
                set.add(key(true, i));
        }

        return set;
    }

    /**
     * @return Cache.
     */
    private IgniteCache cache() {
        return G.ignite(NODE_CLI).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * Get key.
     *
     * @param comparable Whether it should be comparable.
     * @param id ID.
     * @return Key.
     */
    private static TestKey key(boolean comparable, int id) {
        return comparable ? new TestKeyComparable(id) : new TestKey(id);
    }

    /**
     * Create configuration.
     *
     * @param name Node name.
     * @param client Client mode flag.
     * @return Configuration.
     */
    private static IgniteConfiguration configuration(String name, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(name);

        cfg.setClientMode(client);

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test key.
     */
    private static class TestKey {
        /** ID. */
        private int id;

        /**
         * Constructor.
         *
         * @param id ID.
         */
        public TestKey(int id) {
            this.id = id;
        }

        /**
         * @return ID.
         */
        public int id() {
            return id;
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            TestKey key = (TestKey)o;

            return id == key.id;
        }

        /** */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     * Test key implementing comparable interface.
     */
    private static class TestKeyComparable extends TestKey implements Comparable<TestKey> {
        /**
         * Constructor.
         *
         * @param id ID.
         */
        private TestKeyComparable(int id) {
            super(id);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull TestKey o) {
            return id() - o.id();
        }
    }

    /**
     * Test key comparator.
     */
    private static class TestKeyComparator implements Comparator<TestKey> {
        /** {@inheritDoc} */
        @Override public int compare(TestKey o1, TestKey o2) {
            return o1.id() - o2.id();
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            return true;
        }

        /** */
        @Override public int hashCode() {
            return 13;
        }
    }
}
