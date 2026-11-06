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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

/**
 * {@code GridCacheMapEntry#rowSize()} must not throw NPE when the value is an unmarshalled
 * {@code BinaryObjectImpl} or a POJO whose key has not yet been marshalled on the reservation path.
 */
public class PageEvictionRowSizeBinaryObjectTest extends PageEvictionAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_2_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testPojoPutTxDoesNotNpeInRowSize() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestObject> cache = ignite.createCache(
            new CacheConfiguration<Integer, TestObject>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC));

        int entries = 15_000;

        for (int i = 1; i <= entries; i++)
            cache.put(i, new TestObject(PAGE_SIZE / 6));

        assertTrue("Eviction must have started", isEvictionsStarted(ignite));

        int probeKey = entries + 1;

        cache.put(probeKey, new TestObject(PAGE_SIZE / 6));

        assertNotNull(cache.get(probeKey));
    }

    /** */
    @Test
    public void testPojoDataStreamDoesNotNpeInRowSize() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, TestObject> cache = ignite.createCache(new CacheConfiguration<Integer, TestObject>(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32)));

        int entries = 15_000;

        try (IgniteDataStreamer<Integer, TestObject> streamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            streamer.allowOverwrite(true);

            for (int i = 1; i <= entries; i++)
                streamer.addData(i, new TestObject(PAGE_SIZE / 6));
        }

        assertTrue("Eviction must have started", isEvictionsStarted(ignite));

        int probeKey = entries + 1;

        cache.put(probeKey, new TestObject(PAGE_SIZE / 6));

        assertNotNull(cache.get(probeKey));
    }
}
