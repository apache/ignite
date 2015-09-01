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

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;

/**
 *
 */
public class IgniteCacheTtlCleanupSelfTest extends GridCacheAbstractSelfTest {
    /** Number of partitions. */
    private static final int PART_NUM = 10;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 10));

        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeferredDeleteTtl() throws Exception {
        IgniteCache<Object, Object> cache = grid(0).cache(null)
            .withExpiryPolicy(new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 5)));

        int cnt = GridDhtLocalPartition.MAX_DELETE_QUEUE_SIZE / PART_NUM + 100;

        for (long i = 0; i < cnt; i++)
            grid(0).cache(null).put(i * PART_NUM, i);

        for (int i = 0; i < cnt; i++)
            cache.put(i * PART_NUM, i);

        // Wait 5 seconds.
        Thread.sleep(6_000);

        assertEquals(cnt, grid(0).cache(null).size());

        GridCacheAdapter<Object, Object> cacheAdapter = ((IgniteKernal)grid(0)).internalCache(null);

        IgniteCacheObjectProcessor cacheObjects = cacheAdapter.context().cacheObjects();

        CacheObjectContext cacheObjCtx = cacheAdapter.context().cacheObjectContext();

        for (int i = 0; i < 100; i++)
            assertNull(cacheAdapter.map().getEntry(cacheObjects.toCacheKeyObject(cacheObjCtx, i, true)));
    }
}