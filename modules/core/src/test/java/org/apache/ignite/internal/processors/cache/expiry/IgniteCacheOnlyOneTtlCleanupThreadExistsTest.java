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

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Checks that one and only one Ttl cleanup worker thread must exists, and only
 * if at least one cache with set 'eagerTtl' flag exists.
 */
public class IgniteCacheOnlyOneTtlCleanupThreadExistsTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME1 = "cache-1";

    /** */
    private static final String CACHE_NAME2 = "cache-2";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOnlyOneTtlCleanupThreadExists() throws Exception {
        try (final Ignite g = startGrid(0)) {
            checkCleanupThreadExists(false);

            g.createCache(createCacheConfiguration(CACHE_NAME1, false));

            checkCleanupThreadExists(false);

            g.createCache(createCacheConfiguration(CACHE_NAME2, true));

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME1);

            checkCleanupThreadExists(true);

            g.createCache(createCacheConfiguration(CACHE_NAME1, true));

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME1);

            checkCleanupThreadExists(true);

            g.destroyCache(CACHE_NAME2);

            checkCleanupThreadExists(false);
        }
    }

    /**
     * @param name Cache name.
     * @param eagerTtl Eager ttl falg.
     * @return Cache configuration.
     */
    private CacheConfiguration createCacheConfiguration(@NotNull String name, boolean eagerTtl) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setEagerTtl(eagerTtl);
        ccfg.setName(name);

        return ccfg;
    }

    /**
     * @param exists {@code True} if ttl cleanup worker thread expected.
     * @throws Exception If failed.
     */
    private void checkCleanupThreadExists(boolean exists) throws Exception {
        int cnt = 0;

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("ttl-cleanup-worker"))
                cnt++;
        }

        if (cnt > 1)
            fail("More then one ttl cleanup worker threads exists");

        if (exists)
            assertEquals("Ttl cleanup thread does not exist", cnt, 1);
        else
            assertEquals("Ttl cleanup thread exists", cnt, 0);
    }
}
