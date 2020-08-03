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
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Tests dht mapping.
 */
public class GridCacheDhtMappingSelfTest extends GridCommonAbstractTest {
    /** Number of key backups. */
    private static final int BACKUPS = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testMapping() throws Exception {
        int nodeCnt = 5;

        startGridsMultiThreaded(nodeCnt);

        IgniteCache<Integer, Integer> cache = grid(nodeCnt - 1).cache(DEFAULT_CACHE_NAME);

        int kv = 1;

        cache.put(kv, kv);

        int cnt = 0;

        for (int i = 0; i < nodeCnt; i++) {
            Ignite g = grid(i);

            GridDhtCacheAdapter<Integer, Integer> dht = ((GridNearCacheAdapter<Integer, Integer>)
                ((IgniteKernal)g).<Integer, Integer>internalCache(DEFAULT_CACHE_NAME)).dht();

            if (localPeek(dht, kv) != null) {
                info("Key found on node: " + g.cluster().localNode().id());

                cnt++;
            }
        }

        // Test key should be on primary and backup node only.
        assertEquals(1 + BACKUPS, cnt);
    }
}
