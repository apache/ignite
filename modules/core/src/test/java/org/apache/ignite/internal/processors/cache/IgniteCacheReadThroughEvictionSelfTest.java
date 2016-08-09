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
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteCacheConfigVariationsAbstractTest;

import javax.cache.configuration.Factory;
import javax.cache.expiry.AccessedExpiryPolicy;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheReadThroughEvictionSelfTest extends IgniteCacheConfigVariationsAbstractTest {
    /** */
    private static final int TIMEOUT = 400;

    /** */
    private static final int KEYS = 100;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        storeStgy.resetStore();
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughWithExpirePolicy() throws Exception {
        Ignite ig = testedGrid();

        CacheConfiguration<Object, Object> cc = variationConfig("expire");

        IgniteCache<Object, Object> cache = ig.createCache(cc);

        try {
            ExpiryPolicy exp = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, TIMEOUT));

            for (int i = 0; i < KEYS; i++)
                cache.withExpiryPolicy(exp).put(key(i), value(i));

            U.sleep(TIMEOUT);

            waitEmpty(cc.getName());

            exp = new AccessedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, TIMEOUT));

            for (int i = 0; i < KEYS; i++) {
                assertEquals(value(i), cache.get(key(i)));

                cache.withExpiryPolicy(exp).get(key(i));
            }

            U.sleep(TIMEOUT);

            waitEmpty(cc.getName());

            for (int i = 0; i < KEYS; i++)
                assertEquals(value(i), cache.get(key(i)));
        }
        finally {
            destroyCacheSafe(ig, cc.getName());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughExpirePolicyConfigured() throws Exception {
        Ignite ig = testedGrid();

        CacheConfiguration<Object, Object> cc = variationConfig("expireConfig");

        cc.setExpiryPolicyFactory(new ExpirePolicyFactory());

        IgniteCache<Object, Object> cache = ig.createCache(cc);

        try {
            for (int i = 0; i < KEYS; i++)
                cache.put(key(i), value(i));

            U.sleep(TIMEOUT);

            waitEmpty(cc.getName());

            for (int i = 0; i < KEYS; i++) {
                assertEquals(value(i), cache.get(key(i)));

                // Access expiry.
                cache.get(key(i));
            }

            U.sleep(TIMEOUT);

            waitEmpty(cc.getName());

            for (int i = 0; i < KEYS; i++)
                assertEquals(value(i), cache.get(key(i)));

            for (int i = 0; i < KEYS; i++) {
                assertEquals(value(i), cache.get(key(i)));

                // Update expiry.
                cache.put(key(i), value(i));
            }

            U.sleep(TIMEOUT);

            waitEmpty(cc.getName());

            for (int i = 0; i < KEYS; i++)
                assertEquals(value(i), cache.get(key(i)));
        }
        finally {
            destroyCacheSafe(ig, cc.getName());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughEvictionPolicy() throws Exception {
        Ignite ig = testedGrid();

        CacheConfiguration<Object, Object> cc = variationConfig("eviction");

        cc.setEvictionPolicy(new FifoEvictionPolicy(1));

        if (cc.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED)
            cc.setOffHeapMaxMemory(2 * 1024);

        final IgniteCache<Object, Object> cache = ig.createCache(cc);

        try {
            for (int i = 0; i < KEYS; i++)
                cache.put(key(i), value(i));

            assertTrue(GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    int size = cache.size(CachePeekMode.ONHEAP);
                    int offheapSize = cache.size(CachePeekMode.OFFHEAP);

                    System.out.println("Cache [onHeap=" + size + ", offHeap=" + offheapSize + ']');

                    return size <= testsCfg.gridCount() && offheapSize < KEYS;
                }
            }, getTestTimeout()));

            for (int i = 0; i < KEYS; i++)
                assertEquals(value(i), cache.get(key(i)));
        }
        finally {
            destroyCacheSafe(ig, cc.getName());
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testReadThroughSkipStore() throws Exception {
        Ignite ig = testedGrid();

        CacheConfiguration<Object, Object> cc = variationConfig("skipStore");

        final IgniteCache<Object, Object> cache = ig.createCache(cc);

        try {
            for (int i = 0; i < KEYS; i++) {
                cache.put(key(i), value(i));

                cache.withSkipStore().remove(key(i));
            }

            waitEmpty(cc.getName());

            for (int i = 0; i < KEYS; i++)
                assertEquals(value(i), cache.get(key(i)));
        }
        finally {
            destroyCacheSafe(ig, cc.getName());
        }
    }

    /**
     * @return Variation test configuration.
     */
    private CacheConfiguration<Object, Object> variationConfig(String suffix) {
        CacheConfiguration ccfg = testsCfg.configurationFactory().cacheConfiguration(getTestGridName(testedNodeIdx));

        ccfg.setName(cacheName() + "_" + suffix);

        return ccfg;
    }

    /**
     * @throws Exception if failed.
     */
    private void waitEmpty(final String name) throws Exception {
        boolean success = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite g : G.allGrids()) {
                    GridCacheAdapter<Object, Object> cache = ((IgniteEx)g).context().cache().internalCache(name);

                    if (cache == null)
                        return true;

                    if (!cache.isEmpty())
                        return false;

                    if (cache.context().offheap().entriesCount(null) > 0)
                        return false;
                }

                return true;
            }
        }, getTestTimeout());

        assertTrue("Failed to wait for the cache to be empty", success);
    }

    /**
     * @param ig Ignite.
     * @param cacheName Cache name to destroy.
     * @throws IgniteCheckedException If failed.
     */
    private void destroyCacheSafe(Ignite ig, final String cacheName) throws IgniteCheckedException {
        ig.destroyCache(cacheName);

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                for (Ignite g : G.allGrids()) {
                    IgniteInternalCache<Object, Object> cache = ((IgniteEx)g).context().cache().cache(cacheName);

                    if (cache != null)
                        return false;
                }

                return true;
            }
        }, getTestTimeout());
    }

    /**
     *
     */
    private static class ExpirePolicyFactory implements Factory<ExpiryPolicy> {
        /** {@inheritDoc} */
        @Override public ExpiryPolicy create() {
            return new ExpiryPolicy() {
                @Override public Duration getExpiryForCreation() {
                    return new Duration(TimeUnit.MILLISECONDS, TIMEOUT);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForAccess() {
                    return new Duration(TimeUnit.MILLISECONDS, TIMEOUT);
                }

                /** {@inheritDoc} */
                @Override public Duration getExpiryForUpdate() {
                    return new Duration(TimeUnit.MILLISECONDS, TIMEOUT);
                }
            };
        }
    }
}
