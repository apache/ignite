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

package org.apache.ignite.internal.processors.cache.query;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CachePeekMode.OFFHEAP;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;

/**
 *
 */
public class ScanQueryOffheapExpiryPolicySelfTest extends GridCommonAbstractTest {

    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Entries count */
    private static final int ENTRIES_CNT = 1024;

    /** CAche name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 10)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES_CNT);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntriesMovedFromOnHeap() throws Exception {
        Ignite ignite0 = grid(0);
        Ignite ignite1 = grid(1);

        IgniteCache<Integer, Integer> cache0 = ignite0.cache(CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = ignite1.cache(CACHE_NAME);

        populateCache(cache0);

        assertEquals(0, cache0.localSize(ONHEAP));
        assertEquals(0, cache1.localSize(ONHEAP));

        assertEquals(ENTRIES_CNT, cache0.localSize(OFFHEAP) + cache1.localSize(OFFHEAP));

        cache0.query(new ScanQuery<>()).getAll();
        cache1.query(new ScanQuery<>()).getAll();

        assertEquals(0, cache0.localSize(ONHEAP));
        assertEquals(0, cache1.localSize(ONHEAP));

        assertEquals(ENTRIES_CNT, cache0.localSize(OFFHEAP) + cache1.localSize(OFFHEAP));
    }

    /**
     * @param cache Cache instance.
     */
    private static void populateCache(IgniteCache<Integer, Integer> cache) {
        for (int i = 0; i < ENTRIES_CNT; i++)
            cache.put(i, i);
    }
}
