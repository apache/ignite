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

package org.apache.ignite.internal.processors.cache.store;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;

import javax.cache.configuration.Factory;

/**
 * Tests that write behind store is updated if client does not have store.
 */
public class IgnteCacheWriteThroughStoreFailTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.CLOCK;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected Factory<CacheStore> cacheStoreFactory() {
        return new TestStoreFactory();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailOnPut() throws Exception {
        Ignite client = grid(0);

        IgniteCache<Integer, Integer> cache = client.cache(null);

        TestStore.shouldFail = true;

        try {
            cache.put(1, 2);
            assert false;
        }
        catch (Exception e) {
        }

        TestStore.shouldFail = false;

        assertNull(cache.get(1));
    }
}