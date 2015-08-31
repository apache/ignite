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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test that in {@link CacheMode#PARTITIONED} mode cache writes values only to the near cache store. <p/> This check
 * is needed because in current implementation if {@link org.apache.ignite.internal.processors.cache.store.GridCacheWriteBehindStore} assumes that and user store is
 * wrapped only in near cache (see {@link GridCacheProcessor} init logic).
 */
@SuppressWarnings({"unchecked"})
public class GridCachePartitionedWritesTest extends GridCommonAbstractTest {
    /** Cache store. */
    private CacheStore store;

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);

        assert store != null;

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        store = null;

        super.afterTest();
    }

    /** @throws Exception If test fails. */
    public void testWrite() throws Exception {
        final AtomicInteger putCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        store = new CacheStoreAdapter<Object, Object>() {
            @Override public Object load(Object key) {
                info(">>> Get [key=" + key + ']');

                return null;
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> entry) {
                putCnt.incrementAndGet();
            }

            @Override public void delete(Object key) {
                rmvCnt.incrementAndGet();
            }
        };

        startGrid();

        IgniteCache<Integer, String> cache = jcache();

        try {
            cache.get(1);

            Transaction tx = grid().transactions().txStart();

            try {
                for (int i = 1; i <= 10; i++)
                    cache.put(i, Integer.toString(i));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert cache.size() == 10;

            assert putCnt.get() == 10;

            tx = grid().transactions().txStart();

            try {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.getAndRemove(i);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert rmvCnt.get() == 10;
        }
        finally {
            stopGrid();
        }
    }
}