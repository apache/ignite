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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheReadThroughEvictionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        if (F.eq(gridName, getTestGridName(GRID_CNT - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        TestStore.storeMap.clear();
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = new CacheConfiguration();

        cc.setAtomicityMode(atomicityMode());
        cc.setLoadPreviousValue(false);
        cc.setCacheMode(cacheMode());
        cc.setMemoryMode(cacheMemoryMode());
        cc.setReadThrough(true);
        cc.setWriteThrough(true);
        cc.setBackups(1);

        cc.setCacheStoreFactory(new TestStoreFactory());

        return cc;
    }

    /** {@inheritDoc} */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    protected CacheMemoryMode cacheMemoryMode() {
        return CacheMemoryMode.ONHEAP_TIERED;
    }

    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("unchecked")
    public void testReadThroughWithExpirePolicy() throws Exception {
        Ignite ig = ignite(GRID_CNT - 1);

        CacheConfiguration<Object, Object> cc = cacheConfiguration();

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(cc);

        try {
            ExpiryPolicy exp = new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1));

            for (int i = 0; i < 1000; i++)
                cache.withExpiryPolicy(exp).put(i, i);

            U.sleep(1_000);

            waitEmpty();

            exp = new AccessedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1));

            for (int i = 0; i < 1000; i++) {
                assertEquals(i, ig.cache(null).get(i));

                ig.cache(null).withExpiryPolicy(exp).get(i);
            }

            U.sleep(1_000);

            waitEmpty();

            for (int i = 0; i < 1000; i++)
                assertEquals(i, ig.cache(null).get(i));
        }
        finally {
            ig.destroyCache(null);
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughExpirePolicyConfigured() throws Exception {
        Ignite ig = ignite(GRID_CNT - 1);

        CacheConfiguration<Object, Object> cc = cacheConfiguration();

        cc.setExpiryPolicyFactory(new ExpirePolicyFactory());

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(cc);

        try {
            int keyCnt = 1000;

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, i);

            U.sleep(1_000);

            waitEmpty();

            for (int i = 0; i < keyCnt; i++) {
                assertEquals(i, ig.cache(null).get(i));

                // Access expiry.
                ig.cache(null).get(i);
            }

            U.sleep(1_000);

            waitEmpty();

            for (int i = 0; i < keyCnt; i++)
                assertEquals(i, ig.cache(null).get(i));

            for (int i = 0; i < keyCnt; i++) {
                assertEquals(i, ig.cache(null).get(i));

                // Update expiry.
                ig.cache(null).put(i, i);
            }

            U.sleep(1_000);

            waitEmpty();

            for (int i = 0; i < keyCnt; i++)
                assertEquals(i, ig.cache(null).get(i));
        }
        finally {
            ig.destroyCache(null);
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughEvictionPolicy() throws Exception {
        Ignite ig = ignite(GRID_CNT - 1);

        CacheConfiguration<Object, Object> cc = cacheConfiguration();

        cc.setEvictionPolicy(new FifoEvictionPolicy(1));

        final IgniteCache<Object, Object> cache = ig.getOrCreateCache(cc);

        try {
            for (int i = 0; i < 1000; i++)
                cache.put(i, i);

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    int size = cache.size();

                    System.out.println("Cache size: " + size);

                    return size <= GRID_CNT;
                }
            }, getTestTimeout()));

            for (int i = 0; i < 1000; i++)
                assertEquals(i, cache.get(i));
        }
        finally {
            ig.destroyCache(null);
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughSkipStore() throws Exception {
        Ignite ig = ignite(GRID_CNT - 1);

        CacheConfiguration<Object, Object> cc = cacheConfiguration();

        final IgniteCache<Object, Object> cache = ig.getOrCreateCache(cc);

        try {
            for (int i = 0; i < 1000; i++) {
                cache.put(i, i);

                cache.withSkipStore().remove(i);
            }

            waitEmpty();

            for (int i = 0; i < 1000; i++)
                assertEquals(i, cache.get(i));
        }
        finally {
            ig.destroyCache(null);
        }
    }

    /**
     * @throws Exception if failed.
     */
    private void waitEmpty() throws Exception {
        boolean success = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (int g = 0; g < GRID_CNT - 1; g++) {
                    if (!grid(g).context().cache().internalCache().isEmpty())
                        return false;
                }

                return true;
            }
        }, getTestTimeout());

        assertTrue("Failed to wait for the cache to be empty", success);
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore> {
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    private static class ExpirePolicyFactory implements Factory<ExpiryPolicy> {
        /** {@inheritDoc} */
        @Override public ExpiryPolicy create() {
            return new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.SECONDS, 1);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.SECONDS, 1);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.SECONDS, 1);
                }
            };
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter {
        /** */
        private static Map<Object, Object> storeMap = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            storeMap.remove(key);
        }
    }
}
