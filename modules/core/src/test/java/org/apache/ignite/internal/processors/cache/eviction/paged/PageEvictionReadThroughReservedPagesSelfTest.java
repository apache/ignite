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

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListReservedPagesAbstractTest;
import org.junit.Test;

/**
 * Read-through loads go through {@code versionedValue}, which must reserve pages before the entry lock and release the
 * remainder after the insert. After loads complete and the cluster quiesces, {@code reservedPages} must be 0.
 */
public class PageEvictionReadThroughReservedPagesSelfTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Read-through loads trigger {@code versionedValue}; {@code reservedPages} must be released.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReservedPagesReleasedAfterReadThrough() throws Exception {
        startGrid(0);

        CacheConfiguration<Integer, byte[]> cfg = new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory());

        IgniteCache<Integer, byte[]> cache = grid(0).getOrCreateCache(cfg);

        for (int i = 1; i <= 20_000; i++)
            cache.get(i);

        assertTrue("Eviction must have started during read-through loads", isEvictionsStarted(grid(0)));

        byte[] val = cache.get(1);

        assertNotNull("Value for key 1 must not be null", val);
        assertEquals(PAGE_SIZE - 200, val.length);

        FreeListReservedPagesAbstractTest.assertAllReleased(DEFAULT_CACHE_NAME, 10_000);
    }

    /**
     * Read-through on a 2-node cluster with backups; {@code reservedPages} must be released on every node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReservedPagesReleasedAfterReadThroughOnBackup() throws Exception {
        startGrids(2);

        CacheConfiguration<Integer, byte[]> cfg = new CacheConfiguration<Integer, byte[]>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setBackups(1)
            .setReadThrough(true)
            .setCacheStoreFactory(new TestStoreFactory());

        IgniteCache<Integer, byte[]> cache = grid(0).getOrCreateCache(cfg);

        awaitPartitionMapExchange();

        for (int i = 1; i <= 15_000; i++)
            cache.get(i);

        assertTrue("Eviction must have started", isEvictionsStarted(grid(0)));

        FreeListReservedPagesAbstractTest.assertAllReleased(DEFAULT_CACHE_NAME, 10_000);
    }

    /** */
    private static class TestStoreFactory implements Factory<TestCacheStore> {
        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore();
        }
    }

    /** */
    private static class TestCacheStore extends CacheStoreAdapter<Integer, byte[]> {
        /** {@inheritDoc} */
        @Override public byte[] load(Integer key) throws CacheLoaderException {
            // Fills most of a page so ~20k entries exceed the 20 MB region and trigger eviction.
            return new byte[PAGE_SIZE - 200];
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends byte[]> entry) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
