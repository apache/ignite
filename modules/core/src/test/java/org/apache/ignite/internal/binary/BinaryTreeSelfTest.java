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

import java.util.Collections;
import java.util.Comparator;
import java.util.TreeMap;
import java.util.TreeSet;

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
        super.afterTestsStopped();

        G.stop(NODE_CLI, true);
        G.stop(NODE_SRV, true);
    }

    /**
     * Test {@code TreeMap} data structure.
     *
     * @throws Exception If failed.
     */
    public void testTreeMapRegularNoComparator() throws Exception {
        checkTreeMap(false, false);
    }

    /**
     * Test {@code TreeMap} data structure with comparator.
     *
     * @throws Exception If failed.
     */
    public void testTreeMapRegularComparator() throws Exception {
        checkTreeMap(false, true);
    }

    /**
     * Test {@code TreeMap} data structure with binary mode.
     *
     * @throws Exception If failed.
     */
    public void testTreeMapBinaryNoComparator() throws Exception {
        checkTreeMap(true, false);
    }

    /**
     * Test {@code TreeMap} data structure with binary mode and comparator.
     *
     * @throws Exception If failed.
     */
    public void testTreeMapBinaryComparator() throws Exception {
        checkTreeMap(true, true);
    }

    /**
     * Check {@code TreeMap} data structure.
     *
     * @param useBinary Whether to go through binary mode.
     * @param useComparator Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeMap(boolean useBinary, boolean useComparator) throws Exception {
        // Populate map.
        TreeMap<TestKey, Integer> map;

        if (useComparator) {
            map = new TreeMap<>(new TestKeyComparator());

            for (int i = 0; i < SIZE; i++)
                map.put(key(false, i), i);
        }
        else {
            map = new TreeMap<>();

            for (int i = 0; i < SIZE; i++)
                map.put(key(true, i), i);
        }

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
    }

    /**
     * Test {@code TreeSet} data structure.
     *
     * @throws Exception If failed.
     */
    public void testTreeSetRegularNoComparator() throws Exception {
        checkTreeSet(false, false);
    }

    /**
     * Test {@code TreeSet} data structure with comparator.
     *
     * @throws Exception If failed.
     */
    public void testTreeSetRegularComparator() throws Exception {
        checkTreeSet(false, true);
    }

    /**
     * Test {@code TreeSet} data structure with binary mode.
     *
     * @throws Exception If failed.
     */
    public void testTreeSetBinaryNoComparator() throws Exception {
        checkTreeSet(true, false);
    }

    /**
     * Test {@code TreeSet} data structure with binary mode and comparator.
     *
     * @throws Exception If failed.
     */
    public void testTreeSetBinaryComparator() throws Exception {
        checkTreeSet(true, true);
    }

    /**
     * Check {@code TreeSet} data structure.
     *
     * @param useBinary Whether to go through binary mode.
     * @param useComparator Whether comparator should be used.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkTreeSet(boolean useBinary, boolean useComparator) throws Exception {
        // Populate set.
        TreeSet<TestKey> set;

        if (useComparator) {
            set = new TreeSet<>(new TestKeyComparator());

            for (int i = 0; i < SIZE; i++)
                set.add(key(false, i));
        }
        else {
            set = new TreeSet<>();

            for (int i = 0; i < SIZE; i++)
                set.add(key(true, i));
        }

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
    }
}
