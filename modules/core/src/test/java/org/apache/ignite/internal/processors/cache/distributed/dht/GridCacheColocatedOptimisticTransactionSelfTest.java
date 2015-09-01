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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test ensuring that values are visible inside OPTIMISTIC transaction in co-located cache.
 */
public class GridCacheColocatedOptimisticTransactionSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Cache name. */
    private static final String CACHE = "cache";

    /** Key. */
    private static final Integer KEY = 1;

    /** Value. */
    private static final String VAL = "val";

    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grids. */
    private static Ignite[] ignites;

    /** Regular caches. */
    private static IgniteCache<Integer, String>[] caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        CacheConfiguration cc = new CacheConfiguration();

        cc.setName(CACHE);
        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setNearConfiguration(null);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(true);
        cc.setEvictSynchronized(false);

        c.setDiscoverySpi(disco);
        c.setCacheConfiguration(cc);
        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return c;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        ignites = new Ignite[GRID_CNT];
        caches = new IgniteCache[GRID_CNT];

        for (int i = 0; i < GRID_CNT; i++) {
            ignites[i] = startGrid(i);

            caches[i] = ignites[i].cache(CACHE);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        caches = null;
        ignites = null;
    }

    /**
     * Perform test.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticTransaction() throws Exception {
        for (IgniteCache<Integer, String> cache : caches) {
            Transaction tx = cache.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

            try {
                cache.put(KEY, VAL);

                tx.commit();
            }
            finally {
                tx.close();
            }

            for (IgniteCache<Integer, String> cacheInner : caches) {
                tx = cacheInner.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

                try {
                    assert F.eq(VAL, cacheInner.get(KEY));

                    tx.commit();
                }
                finally {
                    tx.close();
                }
            }

            tx = cache.unwrap(Ignite.class).transactions().txStart(OPTIMISTIC, REPEATABLE_READ);

            try {
                cache.remove(KEY);

                tx.commit();
            }
            finally {
                tx.close();
            }
        }
    }
}