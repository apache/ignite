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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractPartitionedByteArrayValuesSelfTest;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for byte array values in PARTITIONED-ONLY caches.
 */
public abstract class GridCacheAbstractPartitionedOnlyByteArrayValuesSelfTest extends
    GridCacheAbstractPartitionedByteArrayValuesSelfTest {
    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC = "cache_atomic";

    /** Offheap cache name. */
    protected static final String CACHE_ATOMIC_OFFHEAP = "cache_atomic_offheap";

    /** Offheap tiered cache name. */
    protected static final String CACHE_ATOMIC_OFFHEAP_TIERED = "cache_atomic_offheap_tiered";

    /** Atomic caches. */
    private static IgniteCache<Integer, Object>[] cachesAtomic;

    /** Atomic offheap caches. */
    private static IgniteCache<Integer, Object>[] cachesAtomicOffheap;

    /** Atomic offheap caches. */
    private static IgniteCache<Integer, Object>[] cachesAtomicOffheapTiered;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        CacheConfiguration atomicCacheCfg = cacheConfiguration0();

        atomicCacheCfg.setName(CACHE_ATOMIC);
        atomicCacheCfg.setAtomicityMode(ATOMIC);
        atomicCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        CacheConfiguration atomicOffheapCacheCfg = offheapCacheConfiguration0();

        atomicOffheapCacheCfg.setName(CACHE_ATOMIC_OFFHEAP);
        atomicOffheapCacheCfg.setAtomicityMode(ATOMIC);
        atomicOffheapCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        CacheConfiguration atomicOffheapTieredCacheCfg = offheapTieredCacheConfiguration();

        atomicOffheapTieredCacheCfg.setName(CACHE_ATOMIC_OFFHEAP_TIERED);
        atomicOffheapTieredCacheCfg.setAtomicityMode(ATOMIC);
        atomicOffheapTieredCacheCfg.setAtomicWriteOrderMode(PRIMARY);

        c.setCacheConfiguration(cacheConfiguration(),
            offheapCacheConfiguration(),
            offheapTieredCacheConfiguration(),
            atomicCacheCfg,
            atomicOffheapCacheCfg,
            atomicOffheapTieredCacheCfg);

        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        c.setPeerClassLoadingEnabled(peerClassLoading());

        return c;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        int gridCnt = gridCount();

        cachesAtomic = new IgniteCache[gridCnt];
        cachesAtomicOffheap = new IgniteCache[gridCnt];
        cachesAtomicOffheapTiered = new IgniteCache[gridCnt];

        for (int i = 0; i < gridCount(); i++) {
            cachesAtomic[i] = ignites[i].cache(CACHE_ATOMIC);
            cachesAtomicOffheap[i] = ignites[i].cache(CACHE_ATOMIC_OFFHEAP);
            cachesAtomicOffheapTiered[i] = ignites[i].cache(CACHE_ATOMIC_OFFHEAP_TIERED);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cachesAtomic = null;
        cachesAtomicOffheap = null;
        cachesAtomicOffheapTiered = null;

        super.afterTestsStopped();
    }

    /**
     * Test atomic cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        testAtomic0(cachesAtomic);
    }

    /**
     * Test atomic offheap cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicOffheap() throws Exception {
        testAtomic0(cachesAtomicOffheap);
    }

    /**
     * Test atomic offheap cache.
     *
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        testAtomic0(cachesAtomicOffheapTiered);
    }

    /**
     * INternal routine for ATOMIC cache testing.
     *
     * @param caches Caches.
     * @throws Exception If failed.
     */
    private void testAtomic0(IgniteCache<Integer, Object>[] caches) throws Exception {
        byte[] val = wrap(1);

        for (IgniteCache<Integer, Object> cache : caches) {
            cache.put(KEY_1, val);

            for (IgniteCache<Integer, Object> cacheInner : caches)
                assertArrayEquals(val, (byte[])cacheInner.get(KEY_1));

            cache.remove(KEY_1);

            assertNull(cache.get(KEY_1));
        }
    }
}