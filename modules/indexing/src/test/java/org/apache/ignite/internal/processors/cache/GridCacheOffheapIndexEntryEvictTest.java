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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheOffheapIndexEntryEvictTest extends GridCommonAbstractTest {
    /** */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setNetworkTimeout(2000);

        cfg.setSwapSpaceSpi(new FileSwapSpaceSpi());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setOffHeapMaxMemory(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setMemoryMode(OFFHEAP_TIERED);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setSqlOnheapRowCacheSize(10);
        cacheCfg.setIndexedTypes(Integer.class, TestValue.class);
        cacheCfg.setNearConfiguration(null);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testQueryWhenLocked() throws Exception {
        IgniteCache<Integer, TestValue> cache = grid(0).cache(null);

        List<Lock> locks = new ArrayList<>();

        final int ENTRIES = 1000;

        try {
            for (int i = 0; i < ENTRIES; i++) {
                cache.put(i, new TestValue(i));

                Lock lock = cache.lock(i);

                lock.lock(); // Lock entry so that it should not be evicted.

                locks.add(lock);

                for (int j = 0; j < 3; j++)
                    assertNotNull(cache.get(i));
            }

            checkQuery(cache, "_key >= 100", ENTRIES - 100);
        }
        finally {
            for (Lock lock : locks)
                lock.unlock();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdates() throws Exception {
        final int ENTRIES = 500;

        IgniteCache<Integer, TestValue> cache = grid(0).cache(null);

        for (int i = 0; i < ENTRIES; i++) {
            for (int j = 0; j < 3; j++) {
                cache.getAndPut(i, new TestValue(i));

                assertNotNull(cache.get(i));

                assertNotNull(cache.localPeek(i));
            }

            checkQuery(cache, "_key >= 0", i + 1);
        }

        for (int i = 0; i < ENTRIES; i++) {
            if (i % 2 == 0)
                cache.getAndRemove(i);
            else
                cache.remove(i);

            checkQuery(cache, "_key >= 0", ENTRIES - (i + 1));
        }
    }

    /**
     * @param cache Cache.
     * @param sql Query.
     * @param expCnt Number of expected entries.
     */
    private void checkQuery(IgniteCache<Integer, TestValue> cache, String sql, int expCnt) {
        SqlQuery<Integer, TestValue> qry = new SqlQuery<>(TestValue.class, sql);

        List<Cache.Entry<Integer, TestValue>> res = cache.query(qry).getAll();

        assertEquals(expCnt, res.size());

        for (Cache.Entry<Integer, TestValue> e : res) {
            assertNotNull(e.getKey());

            assertEquals((int)e.getKey(), e.getValue().val);
        }
    }

    /**
     *
     */
    static class TestValue implements Externalizable {
        /** */
        private int val;

        /**
         *
         */
        public TestValue() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readInt();
        }
    }
}