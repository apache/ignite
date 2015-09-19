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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheGenericTestStore;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCacheEvictionTouchSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private EvictionPolicy<?, ?> plc;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TransactionConfiguration txCfg = c.getTransactionConfiguration();

        txCfg.setDefaultTxConcurrency(PESSIMISTIC);
        txCfg.setDefaultTxIsolation(REPEATABLE_READ);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(REPLICATED);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setEvictionPolicy(plc);

        CacheStore store = new GridCacheGenericTestStore<Object, Object>() {
            @Override public Object load(Object key) {
                return key;
            }

            @Override public Map<Object, Object> loadAll(Iterable<?> keys) {
                Map<Object, Object> loaded = new HashMap<>();

                for (Object key : keys)
                    loaded.put(key, key);

                return loaded;
            }
        };

        cc.setCacheStoreFactory(singletonFactory(store));
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setLoadPreviousValue(true);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        plc = null;

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistency() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(500);

        this.plc = plc;

        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            final Random rnd = new Random();

            try (Transaction tx = ignite.transactions().txStart()) {
                int iterCnt = 20;
                int keyCnt = 5000;

                for (int i = 0; i < iterCnt; i++) {
                    int j = rnd.nextInt(keyCnt);

                    // Put or remove?
                    if (rnd.nextBoolean())
                        cache.put(j, j);
                    else
                        cache.remove(j);

                    if (i != 0 && i % 1000 == 0)
                        info("Stats [iterCnt=" + i + ", size=" + cache.size() + ']');
                }

                FifoEvictionPolicy<Integer, Integer> plc0 = (FifoEvictionPolicy<Integer, Integer>)this.plc;

                if (!plc0.queue().isEmpty()) {
                    for (Cache.Entry<Integer, Integer> e : plc0.queue())
                        U.warn(log, "Policy queue item: " + e);

                    fail("Test failed, see logs for details.");
                }

                tx.commit();
            }
        }
        catch (Throwable t) {
            error("Test failed.", t);

            fail("Test failed, see logs for details.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictSingle() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(500);

        this.plc = plc;

        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            assertEquals(100, ((FifoEvictionPolicy)plc).queue().size());

            for (int i = 0; i < 100; i++)
                cache.localEvict(Collections.singleton(i));

            assertEquals(0, ((FifoEvictionPolicy)plc).queue().size());
            assertEquals(0, cache.size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictAll() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(500);

        this.plc = plc;

        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            Collection<Integer> keys = new ArrayList<>(100);

            for (int i = 0; i < 100; i++) {
                cache.put(i, i);

                keys.add(i);
            }

            assertEquals(100, ((FifoEvictionPolicy)plc).queue().size());

            for (Integer key : keys)
                cache.localEvict(Collections.singleton(key));

            assertEquals(0, ((FifoEvictionPolicy)plc).queue().size());
            assertEquals(0, cache.size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReload() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(100);

        this.plc = plc;

        try {
            Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            for (int i = 0; i < 10000; i++)
                load(cache, i, true);

            assertEquals(100, cache.size());
            assertEquals(100, cache.size());
            assertEquals(100, ((FifoEvictionPolicy)plc).queue().size());

            Set<Integer> keys = new TreeSet<>();

            for (int i = 0; i < 10000; i++)
                keys.add(i);

            loadAll(cache, keys, true);

            assertEquals(100, cache.size());
            assertEquals(100, cache.size());
            assertEquals(100, ((FifoEvictionPolicy)plc).queue().size());
        }
        finally {
            stopAllGrids();
        }
    }
}