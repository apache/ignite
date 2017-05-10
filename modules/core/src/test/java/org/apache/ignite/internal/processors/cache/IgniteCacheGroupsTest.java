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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCache1() throws Exception {
        Ignite srv0 = ignite(0);

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
        Ignite srv0 = ignite(0);

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

        return ccfg;
    }
}
