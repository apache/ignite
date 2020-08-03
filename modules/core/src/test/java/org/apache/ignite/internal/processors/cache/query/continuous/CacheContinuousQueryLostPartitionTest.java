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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static javax.cache.configuration.FactoryBuilder.factoryOf;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test from https://issues.apache.org/jira/browse/IGNITE-2384.
 */
public class CacheContinuousQueryLostPartitionTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "test_cache";

    /** Cache name. */
    public static final String TX_CACHE_NAME = "tx_test_cache";

    /** Cache name. */
    public static final String MVCC_TX_CACHE_NAME = "mvcc_tx_test_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(2);

        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return grid(0).cluster().nodes().size() == 2;
            }
        }, 10000L);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxEvent() throws Exception {
        testEvent(TX_CACHE_NAME, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTxEvent() throws Exception {
        testEvent(MVCC_TX_CACHE_NAME, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicEvent() throws Exception {
        testEvent(CACHE_NAME, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxClientEvent() throws Exception {
        testEvent(TX_CACHE_NAME, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTxClientEvent() throws Exception {
        testEvent(MVCC_TX_CACHE_NAME, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicClientEvent() throws Exception {
        testEvent(CACHE_NAME, true);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    public void testEvent(String cacheName, boolean client) throws Exception {
        IgniteCache<Integer, String> cache1 = grid(0).getOrCreateCache(cacheName);

        final AllEventListener<Integer, String> lsnr1 = registerCacheListener(cache1);

        IgniteCache<Integer, String> cache2 = grid(1).getOrCreateCache(cacheName);

        Integer key = primaryKey(cache1);

        cache1.put(key, "1");

        // Note the issue is only reproducible if the second registration is done right
        // here, after the first put() above.
        AllEventListener<Integer, String> lsnr20;

        if (client) {
            IgniteCache<Integer, String> clnCache = startClientGrid(3).getOrCreateCache(cacheName);

            lsnr20 = registerCacheListener(clnCache);
        }
        else
            lsnr20 = registerCacheListener(cache2);

        final AllEventListener<Integer, String> lsnr2 = lsnr20;

        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lsnr1.createdCnt.get() == 1;
            }
        }, 2000L) : "Unexpected number of events: " + lsnr1.createdCnt.get();

        // Sanity check.
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lsnr2.createdCnt.get() == 0;
            }
        }, 2000L) : "Expected no create events, but got: " + lsnr2.createdCnt.get();

        // node2 now becomes the primary for the key.
        stopGrid(0);

        final int prevSize = grid(1).cluster().nodes().size();

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return prevSize - 1 == grid(1).cluster().nodes().size();
            }
        }, 5000L);

        cache2.put(key, "2");

        // Sanity check.
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lsnr1.createdCnt.get() == 1;
            }
        }, 2000L) : "Expected no change here, but got: " + lsnr1.createdCnt.get();

        // Sanity check.
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lsnr2.updatedCnt.get() == 0;
            }
        }, 2000L) : "Expected no update events, but got: " + lsnr2.updatedCnt.get();

        System.out.println(">>>>> " + lsnr2.createdCnt.get());

        // This assertion fails: 0 events get delivered.
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return lsnr2.createdCnt.get() == 1;
            }
        }, 2000L) : "Expected a single event due to '2', but got: " + lsnr2.createdCnt.get();
    }

    /**
     * @param cache Cache.
     * @return Event listener.
     */
    private AllEventListener<Integer, String> registerCacheListener(IgniteCache<Integer, String> cache) {
        AllEventListener<Integer, String> lsnr = new AllEventListener<>();

        cache.registerCacheEntryListener(
            new MutableCacheEntryListenerConfiguration<>(factoryOf(lsnr), null, true, false));

        return lsnr;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cache(TX_CACHE_NAME), cache(CACHE_NAME), cache(MVCC_TX_CACHE_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Integer, String> cache(String cacheName) {
        CacheConfiguration<Integer, String> cfg = new CacheConfiguration<>(cacheName);

        cfg.setCacheMode(PARTITIONED);

        if (cacheName.equals(CACHE_NAME))
            cfg.setAtomicityMode(ATOMIC);
        else if (cacheName.equals(TX_CACHE_NAME))
            cfg.setAtomicityMode(TRANSACTIONAL);
        else
            cfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

        cfg.setRebalanceMode(SYNC);
        cfg.setWriteSynchronizationMode(PRIMARY_SYNC);
        cfg.setBackups(0);

        return cfg;
    }

    /**
     * Event listener.
     */
    public static class AllEventListener<K, V> implements CacheEntryCreatedListener<K, V>,
        CacheEntryUpdatedListener<K, V>, CacheEntryRemovedListener<K, V>, CacheEntryExpiredListener<K, V>,
        Serializable {
        /** */
        final AtomicInteger createdCnt = new AtomicInteger();

        /** */
        final AtomicInteger updatedCnt = new AtomicInteger();

        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            createdCnt.incrementAndGet();

            System.out.printf("onCreate: %s. \n", evts);
        }

        /** {@inheritDoc} */
        @Override public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            System.out.printf("onExpired: %s. \n", evts);
        }

        /** {@inheritDoc} */
        @Override public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            System.out.printf("onRemoved: %s. \n", evts);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> evts) {
            updatedCnt.incrementAndGet();

            System.out.printf("onUpdated: %s.", evts);
        }
    }
}
