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

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

/**
 * Test checks whether cache initialization error on client side
 * doesn't causes hangs and doesn't impact other caches.
 */
public class IgniteClientCacheInitializationFailTest extends GridCommonAbstractTest {
    /** Failed cache name. */
    public static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("server")) {
            CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

            ccfg.setIndexedTypes(Integer.class, String.class);

            ccfg.setName(CACHE_NAME);

            ccfg.setCacheStoreFactory(new BrokenStoreFactory());

            cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheInitialization() throws Exception {
        startGrid("server");
        final Ignite client = startGrid("client");

        checkFailedCache(client);

        checkFineCache(client, CACHE_NAME + 1);

        assertNull(client.cache(CACHE_NAME));
        assertNull(client.getOrCreateCache(CACHE_NAME));

        checkFineCache(client, CACHE_NAME + 2);
    }

    /**
     * @param client Client.
     * @param cacheName Cache name.
     */
    private void checkFineCache(Ignite client, String cacheName) {
        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheName);

        cache.put(1, "1");

        assertEquals("1", cache.get(1));
    }

    /**
     * @param client Client.
     */
    private void checkFailedCache(final Ignite client) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache = client.cache(CACHE_NAME);

                cache.put(1, "1");

                assertEquals("1", cache.get(1));

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     *
     */
    private static class BrokenStoreFactory implements Factory<CacheStore<Integer, String>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Integer, String> create() {
            if ("client".equals(((IgniteThread)Thread.currentThread()).getGridName()))
                throw new RuntimeException("This store factory is broken");

            return new GridCacheTestStore();
        }
    }
}
