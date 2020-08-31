/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 *
 */
public class IgniteChangeGlobalStateCacheTest extends IgniteChangeGlobalStateAbstractTest {
    /**
     *
     */
    @Test
    public void testCheckValueAfterActivation() {
        String cacheName = "my-cache";

        Ignite ig1P = primary(0);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        IgniteCache<String, String> cacheP = ig1P.getOrCreateCache(cacheName);

        cacheP.put("key","value");

        stopAllPrimary();

        ig1B.active(true);

        IgniteCache<String, String> cache1B = ig1B.cache(cacheName);
        IgniteCache<String, String> cache2B = ig2B.cache(cacheName);
        IgniteCache<String, String> cache3B = ig3B.cache(cacheName);

        assertTrue(cache1B != null);
        assertTrue(cache2B != null);
        assertTrue(cache3B != null);

        assertEquals(cache1B.get("key"), "value");
        assertEquals(cache2B.get("key"), "value");
        assertEquals(cache3B.get("key"), "value");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMoreKeyValueAfterActivate() throws Exception {
        String cacheName = "my-cache";

        Ignite ig1P = primary(0);
        Ignite ig2P = primary(1);
        Ignite ig3P = primary(2);

        Ignite ig1B = backUp(0);
        Ignite ig2B = backUp(1);
        Ignite ig3B = backUp(2);

        CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<>(cacheName);

        IgniteCache<String, String> cache1P = ig1P.getOrCreateCache(cacheCfg);

        for (int i = 0; i < 4_000; i++)
            cache1P.put("key" + i, "value" + i);

        IgniteCache<String, String> cache2P = ig2P.cache(cacheName);

        for (int i = 4_000; i < 8_000; i++)
            cache2P.put("key" + i, "value" + i);

        IgniteCache<String, String> cache3P = ig3P.cache(cacheName);

        for (int i = 8_000; i < 12_000; i++)
            cache3P.put("key" + i, "value" + i);

        stopAllPrimary();

        ig1B.active(true);

        IgniteCache<String, String> cache1B = ig1B.cache(cacheName);
        IgniteCache<String, String> cache2B = ig2B.cache(cacheName);
        IgniteCache<String, String> cache3B = ig3B.cache(cacheName);

        assertTrue(cache1B != null);
        assertTrue(cache2B != null);
        assertTrue(cache3B != null);

        for (int i = 0; i < 4_000; i++)
            assertEquals("value" + i, cache1B.get("key" + i));

        for (int i = 4_000; i < 8_000; i++)
            assertEquals("value" + i, cache2B.get("key" + i));

        for (int i = 8_000; i < 12_000; i++)
            assertEquals("value" + i, cache3B.get("key" + i));
    }

    /**
     * @throws Exception if fail.
     */
    @Test
    public void testDeActivateAndActivateCacheValue() throws Exception {
        String chName = "myCache";

        Ignite ig1 = primary(0);
        Ignite ig2 = primary(1);
        Ignite ig3 = primary(2);

        IgniteCache<String, String> cacheExp = ig1.getOrCreateCache(chName);

        cacheExp.put("key", "value");

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        ig2.active(false);

        IgniteEx ex1 = (IgniteEx)ig1;
        IgniteEx ex2 = (IgniteEx)ig2;
        IgniteEx ex3 = (IgniteEx)ig3;

        GridCacheProcessor cache1 = ex1.context().cache();
        GridCacheProcessor cache2 = ex2.context().cache();
        GridCacheProcessor cache3 = ex3.context().cache();

        assertTrue(F.isEmpty(cache1.jcaches()));
        assertTrue(F.isEmpty(cache2.jcaches()));
        assertTrue(F.isEmpty(cache3.jcaches()));

        assertTrue(!ig1.active());
        assertTrue(!ig2.active());
        assertTrue(!ig3.active());

        ig3.active(true);

        assertTrue(ig1.active());
        assertTrue(ig2.active());
        assertTrue(ig3.active());

        IgniteCache<String, String> cacheAct = ig2.cache(chName);

        assertEquals("value", cacheAct.get("key"));
    }
}
