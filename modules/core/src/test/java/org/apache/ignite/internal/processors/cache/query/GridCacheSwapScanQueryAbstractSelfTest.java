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

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.spi.swapspace.file.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMemoryMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        cfg.setCacheConfiguration(cacheConfiguration(ATOMIC_CACHE_NAME, ATOMIC),
            cacheConfiguration(TRANSACTIONAL_CACHE_NAME, TRANSACTIONAL));

        if (portableEnabled()) {
            PortableConfiguration pCfg = new PortableConfiguration();

            pCfg.setClassNames(Arrays.asList(Key.class.getName(), Person.class.getName()));

            cfg.setPortableConfiguration(pCfg);
        }

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, GridCacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);

        ccfg.setSwapEnabled(true);

        ccfg.setMemoryMode(OFFHEAP_TIERED);

        ccfg.setOffHeapMaxMemory(1024); // Set small offheap size to provoke eviction in swap.

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setPortableEnabled(portableEnabled());

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @return Portable enabled flag.
     */
    protected abstract boolean portableEnabled();

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
        checkQuery(grid(0).cache(ATOMIC_CACHE_NAME));

        checkQuery(grid(0).cache(TRANSACTIONAL_CACHE_NAME));
    }

    /**
     * @param cache Cache.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void checkQuery(GridCache cache) throws Exception {
        final int ENTRY_CNT = 500;

        for (int i = 0; i < ENTRY_CNT; i++)
            assertTrue(cache.putx(new Key(i), new Person("p-" + i, i)));

        try {
            GridCacheQuery<Map.Entry<Key, Person>> qry = cache.queries().createScanQuery(
                new IgniteBiPredicate<Key, Person>() {
                    @Override public boolean apply(Key key, Person p) {
                        assertEquals(key.id, (Integer)p.salary);

                        return key.id % 2 == 0;
                    }
                }
            );

            Collection<Map.Entry<Key, Person>> res = qry.execute().get();

            assertEquals(ENTRY_CNT / 2, res.size());

            for (Map.Entry<Key, Person> e : res) {
                Key k = e.getKey();
                Person p = e.getValue();

                assertEquals(k.id, (Integer)p.salary);
                assertEquals(0, k.id % 2);
            }

            qry = cache.queries().createScanQuery(null);

            res = qry.execute().get();

            assertEquals(ENTRY_CNT, res.size());

            checkProjectionFilter(cache, ENTRY_CNT / 2 - 5);

            testMultithreaded(cache, ENTRY_CNT / 2);
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.removex(new Key(i)));
        }
    }

    /**
     * @param cache Cache.
     * @param expCnt Expected entries in query result.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"unchecked", "IfMayBeConditional"})
    private void checkProjectionFilter(GridCache cache, int expCnt) throws Exception {
        GridCacheProjection prj;

        if (portableEnabled()) {
            prj = cache.projection(new IgnitePredicate<GridCacheEntry<PortableObject, PortableObject>>() {
                @Override public boolean apply(GridCacheEntry<PortableObject, PortableObject> e) {
                    Key key = e.getKey().deserialize();
                    Person val = e.peek().deserialize();

                    assertNotNull(e.version());

                    assertEquals(key.id, (Integer)val.salary);

                    return key.id % 100 != 0;
                }
            });
        }
        else {
            prj = cache.projection(new IgnitePredicate<GridCacheEntry<Key, Person>>() {
                @Override public boolean apply(GridCacheEntry<Key, Person> e) {
                    Key key = e.getKey();
                    Person val = e.peek();

                    assertNotNull(e.version());

                    assertEquals(key.id, (Integer)val.salary);

                    return key.id % 100 != 0;
                }
            });
        }

        GridCacheQuery<Map.Entry<Key, Person>> qry = prj.queries().createScanQuery(
            new IgniteBiPredicate<Key, Person>() {
                @Override public boolean apply(Key key, Person p) {
                    assertEquals(key.id, (Integer)p.salary);

                    return key.id % 2 == 0;
                }
            }
        );

        Collection<Map.Entry<Key, Person>> res = qry.execute().get();

        assertEquals(expCnt, res.size());
    }

    /**
     * @param cache Cache.
     * @param expCnt Expected entries in query result.
     * @throws Exception If failed.
     */
    private void testMultithreaded(final GridCache cache, final int expCnt) throws Exception {
        log.info("Starting multithreaded queries.");

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @SuppressWarnings("unchecked")
            @Override public Void call() throws Exception {
                GridCacheQuery<Map.Entry<Key, Person>> qry = cache.queries().createScanQuery(
                    new IgniteBiPredicate<Key, Person>() {
                        @Override public boolean apply(Key key, Person p) {
                            assertEquals(key.id, (Integer)p.salary);

                            return key.id % 2 == 0;
                        }
                    }
                );

                for (int i = 0; i < 250; i++) {
                    Collection<Map.Entry<Key, Person>> res = qry.execute().get();

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
    private void checkQueryPrimitives(GridCache cache) throws Exception {
        final int ENTRY_CNT = 500;

        for (int i = 0; i < ENTRY_CNT; i++)
            assertTrue(cache.putx(String.valueOf(i), (long) i));

        try {
            GridCacheQuery<Map.Entry<String, Long>> qry = cache.queries().createScanQuery(
                new IgniteBiPredicate<String, Long>() {
                    @Override public boolean apply(String key, Long val) {
                        assertEquals(key, String.valueOf(val));

                        return val % 2 == 0;
                    }
                }
            );

            Collection<Map.Entry<String, Long>> res = qry.execute().get();

            assertEquals(ENTRY_CNT / 2, res.size());

            for (Map.Entry<String, Long> e : res) {
                String key = e.getKey();
                Long val = e.getValue();

                assertEquals(key, String.valueOf(val));

                assertEquals(0, val % 2);
            }

            qry = cache.queries().createScanQuery(null);

            res = qry.execute().get();

            assertEquals(ENTRY_CNT, res.size());
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.removex(String.valueOf(i)));
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
    private void checkQueryValueByteArray(GridCache cache) throws Exception {
        final int ENTRY_CNT = 100;

        for (int i = 0; i < ENTRY_CNT; i++)
            assertTrue(cache.putx(i, new byte[i]));

        try {
            GridCacheQuery<Map.Entry<Integer, byte[]>> qry = cache.queries().createScanQuery(
                new IgniteBiPredicate<Integer, byte[]>() {
                    @Override public boolean apply(Integer key, byte[] val) {
                        assertEquals(key, (Integer)val.length);

                        return key % 2 == 0;
                    }
                }
            );

            Collection<Map.Entry<Integer, byte[]>> res = qry.execute().get();

            assertEquals(ENTRY_CNT / 2, res.size());

            for (Map.Entry<Integer, byte[]> e : res) {
                Integer key = e.getKey();
                byte[] val = e.getValue();

                assertEquals(key, (Integer)val.length);

                assertEquals(0, key % 2);
            }

            qry = cache.queries().createScanQuery(null);

            res = qry.execute().get();

            assertEquals(ENTRY_CNT, res.size());
        }
        finally {
            for (int i = 0; i < ENTRY_CNT; i++)
                assertTrue(cache.removex(i));
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
    }
}
