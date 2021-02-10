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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for cache key check.
 */
public class GridCacheKeyCheckSelfTest extends GridCacheAbstractSelfTest {
    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setNearConfiguration(nearConfiguration());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setAtomicityMode(atomicityMode);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetTransactional() throws Exception {
        checkGet(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetAtomic() throws Exception {
        checkGet(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutTransactional() throws Exception {
        checkPut(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutAtomic() throws Exception {
        checkPut(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveTransactional() throws Exception {
        checkRemove(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveAtomic() throws Exception {
        checkRemove(ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkGet(CacheAtomicityMode atomicityMode) throws Exception {
        this.atomicityMode = atomicityMode;

        try {
            IgniteCache<IncorrectCacheKey, String> cache = cache();

            cache.get(new IncorrectCacheKey(0));

            fail("Key without hashCode()/equals() was successfully retrieved from cache.");
        }
        catch (IllegalArgumentException e) {
            info("Catched expected exception: " + e.getMessage());

            assertTrue(e.getMessage().startsWith("Cache key must override hashCode() and equals() methods"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPut(CacheAtomicityMode atomicityMode) throws Exception {
        this.atomicityMode = atomicityMode;

        try {
            IgniteCache<IncorrectCacheKey, String> cache = cache();

            cache.put(new IncorrectCacheKey(0), "test_value");

            fail("Key without hashCode()/equals() was successfully inserted to cache.");
        }
        catch (IllegalArgumentException e) {
            info("Catched expected exception: " + e.getMessage());

            assertTrue(e.getMessage().startsWith("Cache key must override hashCode() and equals() methods"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkRemove(CacheAtomicityMode atomicityMode) throws Exception {
        this.atomicityMode = atomicityMode;

        try {
            IgniteCache<IncorrectCacheKey, String> cache = cache();

            cache.remove(new IncorrectCacheKey(0));

            fail("Key without hashCode()/equals() was successfully used for remove operation.");
        }
        catch (IllegalArgumentException e) {
            info("Catched expected exception: " + e.getMessage());

            assertTrue(e.getMessage().startsWith("Cache key must override hashCode() and equals() methods"));
        }
    }

    /** */
    private IgniteCache<IncorrectCacheKey, String> cache() {
        grid(0).context().cache().internalCache(DEFAULT_CACHE_NAME).forceKeyCheck();

        return grid(0).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * Cache key that doesn't override hashCode()/equals().
     */
    private static final class IncorrectCacheKey {
        /** */
        private int someVal;

        /**
         * @param someVal Some test value.
         */
        private IncorrectCacheKey(int someVal) {
            this.someVal = someVal;
        }

        /**
         * @return Test value.
         */
        public int getSomeVal() {
            return someVal;
        }
    }
}
