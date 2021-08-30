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

package org.apache.ignite.internal.encryption;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/** */
public class EncryptedCachePreconfiguredRestartTest extends EncryptedCacheRestartTest {
    /** */
    private boolean differentCachesOnNodes;

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testDifferentPreconfiguredCachesOnNodes() throws Exception {
        differentCachesOnNodes = true;

        super.testCreateEncryptedCache();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testCreateEncryptedCache() throws Exception {
        differentCachesOnNodes = false;

        super.testCreateEncryptedCache();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        String cacheName = ENCRYPTED_CACHE + (differentCachesOnNodes ? "." + igniteInstanceName : "");

        CacheConfiguration ccfg = new CacheConfiguration(cacheName)
            .setEncryptionEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Cache name.
     */
    @NotNull @Override protected String cacheName() {
        return ENCRYPTED_CACHE + (differentCachesOnNodes ? "." + GRID_1 : "");
    }

    /**
     * Creates encrypted cache.
     */
    @Override protected void createEncryptedCache(IgniteEx grid0, IgniteEx grid1, String cacheName, String groupName) {
        IgniteCache<Long, String> cache = grid0.cache(cacheName());

        for (long i = 0; i < 100; i++)
            cache.put(i, "" + i);
    }
}
