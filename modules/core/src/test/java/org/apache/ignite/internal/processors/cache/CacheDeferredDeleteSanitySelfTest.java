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
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Sanity tests of deferred delete for different cache configurations.
 */
public class CacheDeferredDeleteSanitySelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If fails.
     */
    public void testDeferredDelete() throws Exception {
        testDeferredDelete(LOCAL, ATOMIC, false, false);
        testDeferredDelete(LOCAL, TRANSACTIONAL, false, false);

        testDeferredDelete(PARTITIONED, ATOMIC, false, true);
        testDeferredDelete(PARTITIONED, TRANSACTIONAL, false, true);

        testDeferredDelete(REPLICATED, ATOMIC, false, true);
        testDeferredDelete(REPLICATED, TRANSACTIONAL, false, true);

        // Near
        testDeferredDelete(LOCAL, ATOMIC, true, false);
        testDeferredDelete(LOCAL, TRANSACTIONAL, true, false);

        testDeferredDelete(PARTITIONED, ATOMIC, true, true);
        testDeferredDelete(PARTITIONED, TRANSACTIONAL, true, false);

        testDeferredDelete(REPLICATED, ATOMIC, true, true);
        testDeferredDelete(REPLICATED, TRANSACTIONAL, true, true);
    }

    /**
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near cache enabled.
     * @param expVal Expected deferred delete value.
     */
    @SuppressWarnings("unchecked")
    private void testDeferredDelete(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near, boolean expVal) {
        CacheConfiguration ccfg = new CacheConfiguration()
            .setCacheMode(mode)
            .setAtomicityMode(atomicityMode);

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        IgniteCache cache = null;

        try {
            cache = grid(0).getOrCreateCache(ccfg);

            assertEquals(expVal, ((IgniteCacheProxy)grid(0).cache(null)).context().deferredDelete());
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }
}
