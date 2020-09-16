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

package org.apache.ignite.internal.processors.cache.distributed;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class CacheDetectLostPartitionsTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_CACHE_NAME = "testcache";

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test detect lost partitions on a client node when the cache init after partitions was lost.
     * @throws Exception
     */
    @Test
    public void testDetectLostPartitionsOnClient() throws Exception {
        IgniteEx ig = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache1 = ig.createCache(getCacheConfig(TEST_CACHE_NAME + 1));

        IgniteCache<Object, Object> cache2 = ig.createCache(getCacheConfig(TEST_CACHE_NAME + 2));

        for (int i = 0; i < 1000; i++) {
            cache1.put(i, i);

            cache2.put(i, i);
        }

        IgniteEx client = startClientGrid(2);

        stopGrid(1);

        checkCache(client.cache(TEST_CACHE_NAME + 1));

        checkCache(client.cache(TEST_CACHE_NAME + 2));
    }

    /** */
    private CacheConfiguration<Object, Object> getCacheConfig(String cacheName) {
        return new CacheConfiguration<>(cacheName)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
    }

    /** */
    private void checkCache(IgniteCache<Object, Object> cache) {
        assertFalse(cache.lostPartitions().isEmpty());

        GridTestUtils.assertThrows(null, () -> cache.get(1),
            CacheException.class, "partition data has been lost");

        GridTestUtils.assertThrows(null, () -> cache.put(1, 1),
            IgniteException.class, "partition data has been lost");
    }
}
