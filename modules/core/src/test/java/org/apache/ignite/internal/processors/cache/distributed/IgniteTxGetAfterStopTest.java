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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class IgniteTxGetAfterStopTest extends IgniteCacheAbstractTest {
    /** */
    private CacheMode cacheMode;

    /** */
    private NearCacheConfiguration nearCfg;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return cacheMode;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return nearCfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        getAfterStop(REPLICATED, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitioned() throws Exception {
        getAfterStop(PARTITIONED, new NearCacheConfiguration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedNearDisabled() throws Exception {
        getAfterStop(PARTITIONED, null);
    }

    /**
     * @param cacheMode Cache mode.
     * @param nearCfg Near cache configuration.
     * @throws Exception If failed.
     */
    private void getAfterStop(CacheMode cacheMode, @Nullable NearCacheConfiguration nearCfg) throws Exception {
        this.cacheMode = cacheMode;
        this.nearCfg = nearCfg;

        startGrids();

        IgniteCache<Integer, Integer> cache0 = jcache(0);
        IgniteCache<Integer, Integer> cache1 = jcache(1);

        Integer key0 = primaryKey(cache0);
        Integer key1 = primaryKey(cache1);

        try (Transaction tx = ignite(0).transactions().txStart()) {
            log.info("Put: " + key0);

            cache0.put(key0, key0);

            log.info("Stop node.");

            stopGrid(3);

            log.info("Get: " + key1);

            cache0.get(key1);

            log.info("Commit.");

            tx.commit();
        }

        assertEquals(key0, cache0.get(key0));
        assertNull(cache1.get(key1));
    }
}