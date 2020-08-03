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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CachePeekMode.NEAR;

/**
 *
 */
public class GridCacheNearClientHitTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test-near-cache";

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration() {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>();

        cfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setBackups(1);
        cfg.setCopyOnRead(false);
        cfg.setName(CACHE_NAME);
        cfg.setNearConfiguration(new NearCacheConfiguration<>());

        return cfg;
    }

    /**
     * @return Near cache configuration.
     */
    private NearCacheConfiguration<Object, Object> nearCacheConfiguration() {
        NearCacheConfiguration<Object, Object> cfg = new NearCacheConfiguration<>();

        cfg.setNearEvictionPolicy(new LruEvictionPolicy<>(25000));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPeekAfterPrimaryNodeLeft() throws Exception {
        try {
            Ignite crd = startGrid("coordinator", getConfiguration("coordinator"));

            Ignite client = startClientGrid("client", getConfiguration("client"));

            Ignite srvNode = startGrid("server", getConfiguration("server"));

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = srvNode.getOrCreateCache(cacheConfiguration());

            IgniteCache<Object, Object> nearCache = client.createNearCache(CACHE_NAME, nearCacheConfiguration());

            UUID serverNodeId = srvNode.cluster().localNode().id();

            int remoteKey = 0;
            for (; ; remoteKey++) {
                if (crd.affinity(CACHE_NAME).mapKeyToNode(remoteKey).id().equals(serverNodeId))
                    break;
            }

            cache.put(remoteKey, remoteKey);

            Object value = nearCache.localPeek(remoteKey, NEAR);

            assertNull("The value should not be loaded from a remote node.", value);

            nearCache.get(remoteKey);

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNotNull("The returned value should not be null.", value);

            srvNode.close();

            awaitPartitionMapExchange();

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNull("The value should not be loaded from a remote node.", value);

            value = nearCache.get(remoteKey);

            assertNotNull("The value should be loaded from a remote node.", value);

            value = nearCache.localPeek(remoteKey, NEAR);

            assertNotNull("The returned value should not be null.", value);
        }
        finally {
            stopAllGrids();
        }
    }
}
