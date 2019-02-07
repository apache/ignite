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
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class PageEvictionReadThroughTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionWithReadThroughAtomicReplicated() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.ATOMIC, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionWithReadThroughAtomicLocal() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.ATOMIC, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionWithReadThroughTxReplicated() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.TRANSACTIONAL, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEvictionWithReadThroughTxLocal() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.TRANSACTIONAL, CacheMode.LOCAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testEvictionWithReadThroughMvccTxReplicated() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.REPLICATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testEvictionWithReadThroughMvccTxPartitioned() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.PARTITIONED);
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582,https://issues.apache.org/jira/browse/IGNITE-7956,"
        + "https://issues.apache.org/jira/browse/IGNITE-9530")
    @Test
    public void testEvictionWithReadThroughMvccTxLocal() throws Exception {
        testEvictionWithReadThrough(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT, CacheMode.LOCAL);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param cacheMode Cache mode.
     * @throws Exception If failed.
     */
    private void testEvictionWithReadThrough(CacheAtomicityMode atomicityMode, CacheMode cacheMode) throws Exception {
        startGrid(0);

        CacheConfiguration<Object, Object> cfg = cacheConfig("evict-rebalance", null, cacheMode, atomicityMode,
            CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cfg.setReadThrough(true);
        cfg.setCacheStoreFactory(new TestStoreFactory());

        IgniteCache<Object, Object> cache = ignite(0).getOrCreateCache(cfg);

        for (int i = 1; i <= ENTRIES; i++) {
            cache.get(i);

            if (i % (ENTRIES / 10) == 0)
                System.out.println(">>> Entries: " + i);
        }

        int size = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + size);

        assertTrue(size > 0);

        assertTrue(size < ENTRIES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<TestCacheStore> {
        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore();
        }
    }

    /**
     *
     */
    private static class TestCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            ThreadLocalRandom r = ThreadLocalRandom.current();

            if (r.nextInt() % 5 == 0)
                return new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000)); // Fragmented object.
            else
                return new TestObject(r.nextInt(PAGE_SIZE / 4 - 50)); // Fits in one page.
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
