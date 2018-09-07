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
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public abstract class PageEvictionMultinodeAbstractTest extends PageEvictionAbstractTest {
    /** Cache modes. */
    private static final CacheMode[] CACHE_MODES = {CacheMode.PARTITIONED, CacheMode.REPLICATED};

    /** Atomicity modes. */
    private static final CacheAtomicityMode[] ATOMICITY_MODES = {
        CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

    /** Write modes. */
    private static final CacheWriteSynchronizationMode[] WRITE_MODES = {CacheWriteSynchronizationMode.PRIMARY_SYNC,
        CacheWriteSynchronizationMode.FULL_SYNC, CacheWriteSynchronizationMode.FULL_ASYNC};

    /** Client grid. */
    Ignite clientGrid;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(4, false);

        clientGrid = startGrid("client");
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration configuration = super.getConfiguration(gridName);

        if (gridName.startsWith("client"))
            configuration.setClientMode(true);

        return configuration;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageEviction() throws Exception {
        for (int i = 0; i < CACHE_MODES.length; i++) {
            for (int j = 0; j < ATOMICITY_MODES.length; j++) {
                for (int k = 0; k < WRITE_MODES.length; k++) {
                    if (i + j + Math.min(k, 1) <= 1) {
                        CacheConfiguration<Object, Object> cfg = cacheConfig(
                            "evict" + i + j + k, null, CACHE_MODES[i], ATOMICITY_MODES[j], WRITE_MODES[k]);

                        createCacheAndTestEvcition(cfg);
                    }
                }
            }
        }
    }

    /**
     * @param cfg Config.
     * @throws Exception If failed.
     */
    protected void createCacheAndTestEvcition(CacheConfiguration<Object, Object> cfg) throws Exception {
        IgniteCache<Object, Object> cache = clientGrid.getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            if (r.nextInt() % 5 == 0)
                cache.put(i, new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000))); // Fragmented object.
            else
                cache.put(i, new TestObject(r.nextInt(PAGE_SIZE / 4 - 50))); // Fits in one page.

            if (r.nextInt() % 7 == 0)
                cache.get(r.nextInt(i)); // Touch.
            else if (r.nextInt() % 11 == 0)
                cache.remove(r.nextInt(i)); // Remove.
            else if (r.nextInt() % 13 == 0)
                cache.put(r.nextInt(i), new TestObject(r.nextInt(PAGE_SIZE / 2))); // Update.

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries put: " + i);
        }

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

        // Eviction started, no OutOfMemory occurred, success.
        assertTrue(resultingSize < ENTRIES * 10 / 11);

        clientGrid.destroyCache(cfg.getName());
    }
}
