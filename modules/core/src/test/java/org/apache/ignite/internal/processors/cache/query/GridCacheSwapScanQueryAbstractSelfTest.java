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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests scan query over entries in offheap and swap.
 */
public abstract class GridCacheSwapScanQueryAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final String ATOMIC_CACHE_NAME = "atomicCache";

    /** */
    protected static final String TRANSACTIONAL_CACHE_NAME = "transactionalCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        cfg.setCacheConfiguration(cacheConfiguration(ATOMIC_CACHE_NAME, ATOMIC),
            cacheConfiguration(TRANSACTIONAL_CACHE_NAME, TRANSACTIONAL));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);

        ccfg.setSwapEnabled(true);

        ccfg.setMemoryMode(OFFHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(1024); // Set small offheap size to provoke eviction in swap.

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQuery() throws Exception {
        checkQuery(grid(0).cache(ATOMIC_CACHE_NAME), false);

        checkQuery(grid(0).cache(TRANSACTIONAL_CACHE_NAME), false);

        checkQuery(grid(0).cache(ATOMIC_CACHE_NAME), true);

        checkQuery(grid(0).cache(TRANSACTIONAL_CACHE_NAME), true);
    }

    /**
     * @param cache Cache.
     * @param scanPartitions Scan partitions.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkQuery(IgniteCache cache, boolean scanPartitions) throws Exception {
        final int ENTRY_CNT = 500;

        Map<Integer, Map<Key, Person>> entries = new HashMap<>();

        Affinity<Object> aff = ignite(0).affinity(cache.getName());

        for (int i = 0; i < ENTRY_CNT; i++) {
            Key key = new Key(i);
            Person val = new Person("p-" + i, i);

            int part = aff.partition(key);

            cache.getAndPut(key, val);

            Map<Key, Person> partEntries = entries.get(part);

            if (partEntries == null)
                entries.put(part, partEntries = new HashMap<>());

            partEntries.put(key, val);
        }

        try {
            int partitions = scanPartitions ? aff.partitions() : 1;

            for (int i = 0; i < partitions; i++) {
                ScanQuery<Key, Person> qry = new ScanQuery<>(new IgniteBiPredicate<Key, Person>() {
                    @Override public boolean apply(Key key, Person p) {
                        assertEquals(key.id, (Integer)p.salary);

                        return key.id % 2 == 0;
                    }
                });

                if (scanPartitions)
                    qry.setPartition(i);

                List<Cache.Entry<Key, Person>> res = cache.query(qry).getAll();

                if (!scanPartitions)
                    assertEquals(ENTRY_CNT / 2, res.size());

                for (Cache.Entry<Key, Person> e : res) {
                    Key k = e.getKey();
                    Person p = e.getValue();

                    assertEquals(k.id, (Integer)p.salary);
                    assertEquals(0, k.id % 2);

                    if (scanPartitions) {
                        Map<Key, Person> partEntries = entries.get(i);

                        assertEquals(p, partEntries.get(k));
                    }
                }

                qry = new ScanQuery<>();

                if (scanPartitions)
                    qry.setPartition(i);

                res = cache.query(qry).getAll();

                if (!scanPartitions)
                    assertEquals(ENTRY_CNT, res.size());
            }

            testMultithreaded(cache, ENTRY_CNT / 2);
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.remove(new Key(i)));
        }
    }

    /**
     * @param cache Cache.
     * @param expCnt Expected entries in query result.
     * @throws Exception If failed.
     */
    private void testMultithreaded(final IgniteCache cache, final int expCnt) throws Exception {
        log.info("Starting multithreaded queries.");

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override public Void call() throws Exception {
                ScanQuery<Key, Person> qry = new ScanQuery<>(new IgniteBiPredicate<Key, Person>() {
                    @Override public boolean apply(Key key, Person p) {
                        assertEquals(key.id, (Integer)p.salary);

                        return key.id % 2 == 0;
                    }
                });

                for (int i = 0; i < 250; i++) {
                    List<Cache.Entry<Key, Person>> res = cache.query(qry).getAll();

                    assertEquals(expCnt, res.size());

                    if (i % 50 == 0)
                        log.info("Iteration " + i);
                }

                return null;
            }
        }, 8, "test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryPrimitives() throws Exception {
        checkQueryPrimitives(grid(0).cache(ATOMIC_CACHE_NAME));

        checkQueryPrimitives(grid(0).cache(TRANSACTIONAL_CACHE_NAME));
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkQueryPrimitives(IgniteCache cache) throws Exception {
        final int ENTRY_CNT = 500;

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.getAndPut(String.valueOf(i), (long)i);

        try {
            ScanQuery<String, Long> qry = new ScanQuery<>(new IgniteBiPredicate<String, Long>() {
                @Override public boolean apply(String key, Long val) {
                    assertEquals(key, String.valueOf(val));

                    return val % 2 == 0;
                }
            });

            Collection<Cache.Entry<String, Long>> res = cache.query(qry).getAll();

            assertEquals(ENTRY_CNT / 2, res.size());

            for (Cache.Entry<String, Long> e : res) {
                String key = e.getKey();
                Long val = e.getValue();

                assertEquals(key, String.valueOf(val));

                assertEquals(0, val % 2);
            }

            qry = new ScanQuery<>();

            res = cache.query(qry).getAll();

            assertEquals(ENTRY_CNT, res.size());
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.remove(String.valueOf(i)));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryValueByteArray() throws Exception {
        checkQueryValueByteArray(grid(0).cache(ATOMIC_CACHE_NAME));

        checkQueryValueByteArray(grid(0).cache(TRANSACTIONAL_CACHE_NAME));
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkQueryValueByteArray(IgniteCache cache) throws Exception {
        final int ENTRY_CNT = 100;

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.getAndPut(i, new byte[i]);

        try {
            ScanQuery<Integer, byte[]> qry = new ScanQuery<>(
                new IgniteBiPredicate<Integer, byte[]>() {
                    @Override public boolean apply(Integer key, byte[] val) {
                        assertEquals(key, (Integer)val.length);

                        return key % 2 == 0;
                    }
                });

            Collection<Cache.Entry<Integer, byte[]>> res = cache.query(qry).getAll();

            assertEquals(ENTRY_CNT / 2, res.size());

            for (Cache.Entry<Integer, byte[]> e : res) {
                Integer key = e.getKey();
                byte[] val = e.getValue();

                assertEquals(key, (Integer)val.length);

                assertEquals(0, key % 2);
            }

            qry = new ScanQuery<>();

            res = cache.query(qry).getAll();

            assertEquals(ENTRY_CNT, res.size());
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.remove(i));
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Key {
        /** */
        @SuppressWarnings("PublicField")
        public Integer id;

        /**
         * @param id ID.
         */
        public Key(Integer id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return id.equals(key.id);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id.hashCode();
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Person {
        /** */
        @SuppressWarnings("PublicField")
        public String name;

        /** */
        @SuppressWarnings("PublicField")
        public int salary;

        /**
         * @param name Name.
         * @param salary Salary.
         */
        public Person(String name, int salary) {
            this.name = name;
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            if (salary != person.salary)
                return false;

            return !(name != null ? !name.equals(person.name) : person.name != null);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = name != null ? name.hashCode() : 0;

            return 31 * res + salary;
        }
    }
}