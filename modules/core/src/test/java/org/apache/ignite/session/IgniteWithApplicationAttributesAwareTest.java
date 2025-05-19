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

package org.apache.ignite.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgniteWithApplicationAttributesAwareTest extends GridCommonAbstractTest {
    /** */
    private static final String DEFAULT_CACHE_NAME0 = "default-0";

    /** */
    private static final String DEFAULT_CACHE_CFG_NAME = "default-cfg";

    /** */
    private static final String DEFAULT_CACHE_CFG_NAME0 = "default-cfg-0";

    /** */
    @Test
    public void testMultipleWithApplicationAttributes() throws Exception {
        try (Ignite ign = startGrid()) {
            Map<String, String> attrs = F.asMap("0", "0");

            Ignite appIgn = ign.withApplicationAttributes(attrs);
            Ignite appIgn0 = appIgn.withApplicationAttributes(attrs);

            assertSame(appIgn, appIgn0);

            Ignite appIgn1 = appIgn.withApplicationAttributes(F.asMap("1", "1"));

            assertNotSame(appIgn, appIgn1);
        }
    }

    /** */
    @Test
    public void testNonExistentCache() throws Exception {
        try (Ignite ign = startGrid()) {
            Ignite appIgn = ign.withApplicationAttributes(F.asMap("0", "0"));

            assertNull(appIgn.cache(DEFAULT_CACHE_NAME));
        }
    }

    /** */
    @Test
    public void testReturnSameCacheReference() throws Exception {
        try (Ignite ign = startGrid()) {
            Ignite appIgn = ign.withApplicationAttributes(F.asMap("0", "0"));

            Map<String, IgniteCache<?, ?>> caches = new HashMap<>();

            caches.computeIfAbsent(DEFAULT_CACHE_NAME, appIgn::createCache);
            caches.computeIfAbsent(DEFAULT_CACHE_CFG_NAME, c -> appIgn.createCache(new CacheConfiguration<>(c)));
            caches.computeIfAbsent(DEFAULT_CACHE_NAME0, appIgn::getOrCreateCache);
            caches.computeIfAbsent(DEFAULT_CACHE_CFG_NAME0, c -> appIgn.getOrCreateCache(new CacheConfiguration<>(c)));

            for (Map.Entry<String, IgniteCache<?, ?>> cache: caches.entrySet()) {
                IgniteCache<?, ?> c0 = appIgn.cache(cache.getKey());
                IgniteCache<?, ?> c1 = appIgn.getOrCreateCache(cache.getKey());
                IgniteCache<?, ?> c2 = appIgn.getOrCreateCache(new CacheConfiguration<>(cache.getKey()));
                IgniteCache<?, ?> c3 = appIgn.getOrCreateCaches(F.asList(new CacheConfiguration<>(cache.getKey())))
                    .stream().findFirst().get();

                assertSame(cache.getValue(), c0);
                assertSame(cache.getValue(), c1);
                assertSame(cache.getValue(), c2);
                assertSame(cache.getValue(), c3);
            }
        }
    }

    /** */
    @Test
    public void testCreateAndGetMultipleCaches() throws Exception {
        try (Ignite ign = startGrid()) {
            Ignite appIgn = ign.withApplicationAttributes(F.asMap("0", "0"));

            List<IgniteCache> crtCaches = (List<IgniteCache>)appIgn.createCaches(F.asList(
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME),
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME0)));

            List<IgniteCache> getOrCrtCaches = (List<IgniteCache>)appIgn.getOrCreateCaches(F.asList(
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME),
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME0)));

            IgniteCache<?, ?> cache = appIgn.cache(DEFAULT_CACHE_CFG_NAME);
            IgniteCache<?, ?> cache0 = appIgn.cache(DEFAULT_CACHE_CFG_NAME0);

            assertSame(crtCaches.get(0), getOrCrtCaches.get(0));
            assertSame(crtCaches.get(1), getOrCrtCaches.get(1));
            assertSame(crtCaches.get(0), cache);
            assertSame(crtCaches.get(1), cache0);
        }
    }

    /** */
    @Test
    public void testDestroyCacheCleansReference() throws Exception {
        try (Ignite ign = startGrid()) {
            Ignite appIgn = ign.withApplicationAttributes(F.asMap("0", "0"));

            appIgn.createCache(DEFAULT_CACHE_NAME);
            appIgn.destroyCache(DEFAULT_CACHE_NAME);

            assertNull(appIgn.cache(DEFAULT_CACHE_NAME));

            appIgn.createCaches(F.asList(
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME),
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME0)));

            appIgn.destroyCaches(F.asList(DEFAULT_CACHE_CFG_NAME, DEFAULT_CACHE_CFG_NAME0));

            assertNull(appIgn.cache(DEFAULT_CACHE_CFG_NAME));
            assertNull(appIgn.cache(DEFAULT_CACHE_CFG_NAME0));
        }
    }

    /** */
    @Test
    public void testNearCacheReferences() throws Exception {
        try (Ignite ign = startGrid(0); Ignite cln = startClientGrid(1)) {
            Ignite appClnIgn = cln.withApplicationAttributes(F.asMap("0", "0"));

            // Check createCache.
            IgniteCache<?, ?> cfgCache = appClnIgn.createCache(
                new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME), new NearCacheConfiguration<>());

            IgniteCache<?, ?> c0 = appClnIgn.cache(DEFAULT_CACHE_CFG_NAME);
            IgniteCache<?, ?> c1 = appClnIgn.getOrCreateNearCache(DEFAULT_CACHE_CFG_NAME, new NearCacheConfiguration<>());

            assertSame(cfgCache, c0);
            assertSame(cfgCache, c1);

            // Check getOrCreateCache.
            IgniteCache<?, ?> dftCache = appClnIgn.getOrCreateCache(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME), new NearCacheConfiguration<>());

            IgniteCache<?, ?> c2 = appClnIgn.cache(DEFAULT_CACHE_NAME);
            IgniteCache<?, ?> c3 = appClnIgn.getOrCreateNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());

            assertSame(dftCache, c2);
            assertSame(dftCache, c3);

            // Check createNearCache.
            ign.createCache(new CacheConfiguration<>(DEFAULT_CACHE_CFG_NAME0));
            IgniteCache<?, ?> cache = appClnIgn.createNearCache(DEFAULT_CACHE_CFG_NAME0, new NearCacheConfiguration<>());

            IgniteCache<?, ?> c4 = appClnIgn.cache(DEFAULT_CACHE_CFG_NAME0);

            assertSame(cache, c4);
        }
    }
}
