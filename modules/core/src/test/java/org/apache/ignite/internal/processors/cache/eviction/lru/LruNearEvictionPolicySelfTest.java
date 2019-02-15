/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.eviction.lru;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * LRU near eviction tests (GG-8884).
 */
public class LruNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Cache atomicity mode specified by test. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cc.setCacheMode(PARTITIONED);
        cc.setAtomicityMode(atomicityMode);
        cc.setWriteSynchronizationMode(PRIMARY_SYNC);
        cc.setRebalanceMode(SYNC);
        cc.setBackups(0);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(EVICTION_MAX_SIZE);

        nearCfg.setNearEvictionPolicy(plc);
        cc.setNearConfiguration(nearCfg);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testMvccTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGridsMultiThreaded(GRID_COUNT);

        try {
            Random rand = new Random(0);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataStreamer<Integer, String> ldr = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(rand.nextInt(GRID_COUNT)).cache(DEFAULT_CACHE_NAME);

                assertTrue(cache.get(i).equals(Integer.toString(i)));
            }

            for (int i = 0; i < GRID_COUNT; i++)
                assertTrue("Near cache size " + near(i).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                    near(i).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
