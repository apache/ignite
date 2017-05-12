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

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        //cfg.setLateAffinityAssignment(false);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseCache1() throws Exception {
        startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        IgniteCache c1 = client.createCache(cacheConfiguration(GROUP1, "c1", ATOMIC, 0));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(0, GROUP1, true);

        checkCache(0, "c1");
        checkCache(1, "c1");

        c1.close();

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, false);

        checkCache(0, "c1");

        assertNotNull(client.cache("c1"));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, true);

        checkCache(0, "c1");
        checkCache(1, "c1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches1() throws Exception {
        createDestroyCaches(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches2() throws Exception {
        createDestroyCaches(5);
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void createDestroyCaches(int srvs) throws Exception {
        startGridsMultiThreaded(srvs);

        Ignite srv0 = ignite(0);

        for (int i = 0; i < srvs; i++)
            checkCacheGroup(i, GROUP1, false);

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            srv0.createCache(cacheConfiguration(GROUP1, "cache1", ATOMIC, 2));

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, "cache1");
            }

            srv0.createCache(cacheConfiguration(GROUP1, "cache2", ATOMIC, 2));

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, "cache2");
            }

            srv0.destroyCache("cache1");

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                //checkCache(i, "cache2");
            }

            srv0.destroyCache("cache2");

            for (int i = 0; i < srvs; i++)
                checkCacheGroup(i, GROUP1, false);
        }
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     */
    private void checkCache(int idx, String cacheName) {
        IgniteCache cache = ignite(idx).cache(cacheName);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            Integer key = rnd.nextInt();

            cache.put(key, i);

            assertEquals(i, cache.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCache1() throws Exception {
        Ignite srv0 = startGrid(0);

        {
            IgniteCache<Object, Object> cache1 = srv0.createCache(cacheConfiguration("grp1", "cache1", ATOMIC, 2));
            IgniteCache<Object, Object> cache2 = srv0.createCache(cacheConfiguration("grp1", "cache2", ATOMIC, 2));

            cache1.put(new Key1(1), 1);
            assertEquals(1, cache1.get(new Key1(1)));

            assertEquals(1, cache1.size());
            assertEquals(0, cache2.size());
            //assertFalse(cache2.iterator().hasNext());

            cache2.put(new Key2(1), 2);
            assertEquals(2, cache2.get(new Key2(1)));

            assertEquals(1, cache1.size());
            assertEquals(1, cache2.size());
        }

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache1 = srv1.cache("cache1");
        IgniteCache<Object, Object> cache2 = srv1.cache("cache2");

        assertEquals(1, cache1.localPeek(new Key1(1)));
        assertEquals(2, cache2.localPeek(new Key2(1)));

        assertEquals(1, cache1.localSize());
        assertEquals(1, cache2.localSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCache2() throws Exception {
        Ignite srv0 = startGrid(0);

        {
            IgniteCache<Object, Object> cache1 = srv0.createCache(cacheConfiguration("grp1", "cache1", ATOMIC, 0));
            IgniteCache<Object, Object> cache2 = srv0.createCache(cacheConfiguration("grp1", "cache2", ATOMIC, 0));

            for (int i = 0; i < 10; i++) {
                cache1.put(new Key1(i), 1);
                cache2.put(new Key2(i), 2);
            }
        }

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     *
     */
    static class Key1 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key1(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key1 key = (Key1)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Key2 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key2(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key2 key = (Key2)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    private CacheConfiguration cacheConfiguration(String grpName,
        String name,
        CacheAtomicityMode atomicityMode,
        int backups) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @param idx Node index.
     * @param grpName Cache group name.
     * @param expGrp {@code True} if cache group should be created.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCacheGroup(int idx, final String grpName, final boolean expGrp) throws IgniteCheckedException {
        final IgniteKernal node = (IgniteKernal)ignite(idx);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expGrp == (cacheGroup(node, grpName) != null);
            }
        }, 1000));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }

    /**
     * @param node Node.
     * @param grpName Cache group name.
     * @return Cache group.
     */
    private CacheGroupInfrastructure cacheGroup(IgniteKernal node, String grpName) {
        for (CacheGroupInfrastructure grp : node.context().cache().cacheGroups()) {
            if (grpName.equals(grp.name()))
                return grp;
        }

        return null;
    }
}
