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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * LRU near eviction tests for NEAR_ONLY distribution mode (GG-8884).
 */
public class LruNearOnlyNearEvictionPolicySelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_COUNT = 2;

    /** Maximum size for near eviction policy. */
    private static final int EVICTION_MAX_SIZE = 10;

    /** Node count. */
    private int cnt;

    /** Caching mode specified by test. */
    private CacheMode cacheMode;

    /** Cache atomicity mode specified by test. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cnt = 0;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        if (cnt == 0)
            c.setClientMode(true);
        else {
            CacheConfiguration cc = new CacheConfiguration(DEFAULT_CACHE_NAME);

            cc.setCacheMode(cacheMode);
            cc.setAtomicityMode(atomicityMode);
            cc.setWriteSynchronizationMode(PRIMARY_SYNC);
            cc.setRebalanceMode(SYNC);
            cc.setBackups(0);

            c.setCacheConfiguration(cc);
        }

        ((TcpDiscoverySpi)c.getDiscoverySpi()).setForceServerMode(true);

        cnt++;

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testPartitionedMvccTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        cacheMode = PARTITIONED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedAtomicNearEvictionMaxSize() throws Exception {
        atomicityMode = ATOMIC;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7187,https://issues.apache.org/jira/browse/IGNITE-7956")
    @Test
    public void testReplicatedMvccTransactionalNearEvictionMaxSize() throws Exception {
        atomicityMode = TRANSACTIONAL_SNAPSHOT;
        cacheMode = REPLICATED;

        checkNearEvictionMaxSize();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkNearEvictionMaxSize() throws Exception {
        startGrids(GRID_COUNT);

        try {
            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            LruEvictionPolicy plc = new LruEvictionPolicy();
            plc.setMaxSize(EVICTION_MAX_SIZE);

            nearCfg.setNearEvictionPolicy(plc);

            grid(0).createNearCache(DEFAULT_CACHE_NAME, nearCfg);

            int cnt = 1000;

            info("Inserting " + cnt + " keys to cache.");

            try (IgniteDataStreamer<Integer, String> ldr = grid(1).dataStreamer(DEFAULT_CACHE_NAME)) {
                for (int i = 0; i < cnt; i++)
                    ldr.addData(i, Integer.toString(i));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);

            info("Getting " + cnt + " keys from cache.");

            for (int i = 0; i < cnt; i++) {
                IgniteCache<Integer, String> cache = grid(0).cache(DEFAULT_CACHE_NAME);

                assertTrue(cache.get(i).equals(Integer.toString(i)));
            }

            assertTrue("Near cache size " + near(0).nearSize() + ", but eviction maximum size " + EVICTION_MAX_SIZE,
                near(0).nearSize() <= EVICTION_MAX_SIZE);
        }
        finally {
            stopAllGrids();
        }
    }
}
