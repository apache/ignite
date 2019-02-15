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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Checks that CacheProjection.reload() operations are performed correctly.
 */
@RunWith(JUnit4.class)
public class GridCacheReloadSelfTest extends GridCommonAbstractTest {
    /** Maximum allowed number of cache entries. */
    public static final int MAX_CACHE_ENTRIES = 500;

    /** Number of entries to load from store. */
    public static final int N_ENTRIES = 5000;

    /** Cache name. */
    private static final String CACHE_NAME = "test";

    /** Cache mode. */
    private CacheMode cacheMode;

    /** Near enabled flag. */
    private boolean nearEnabled = true;

    /** */
    @Before
    public void beforeGridCacheReloadSelfTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        if (nearEnabled)
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cacheMode = null;
        nearEnabled = true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.EVICTION);

        if (nearEnabled)
            MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Collections.singleton("127.0.0.1:47500"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();
        cacheCfg.setName(CACHE_NAME);
        cacheCfg.setCacheMode(cacheMode);

        LruEvictionPolicy plc = new LruEvictionPolicy();
        plc.setMaxSize(MAX_CACHE_ENTRIES);

        cacheCfg.setEvictionPolicy(plc);
        cacheCfg.setOnheapCacheEnabled(true);
        cacheCfg.setNearConfiguration(nearEnabled ? new NearCacheConfiguration() : null);

        final CacheStore store = new CacheStoreAdapter<Integer, Integer>() {
            @Override public Integer load(Integer key) {
                return key;
            }

            @Override public void write(javax.cache.Cache.Entry<? extends Integer, ? extends Integer> e) {
                //No-op.
            }

            @Override public void delete(Object key) {
                //No-op.
            }
        };

        cacheCfg.setCacheStoreFactory(singletonFactory(store));
        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);
        cacheCfg.setLoadPreviousValue(true);

        if (cacheMode == PARTITIONED)
            cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * Checks that eviction works with reload() on local cache.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testReloadEvictionLocalCache() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        cacheMode = CacheMode.LOCAL;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near enabled.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testReloadEvictionPartitionedCacheNearEnabled() throws Exception {
        cacheMode = PARTITIONED;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on partitioned cache
     * with near disabled.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testReloadEvictionPartitionedCacheNearDisabled() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        doTest();
    }

    /**
     * Checks that eviction works with reload() on replicated cache.
     *
     * @throws Exception If error occurs.
     */
    @Test
    public void testReloadEvictionReplicatedCache() throws Exception {
        cacheMode = CacheMode.REPLICATED;

        doTest();
    }

    /**
     * Actual test logic.
     *
     * @throws Exception If error occurs.
     */
    private void doTest() throws Exception {
        Ignite ignite = startGrid();

        try {
            IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

            for (int i = 0; i < N_ENTRIES; i++)
                load(cache, i, true);

            assertEquals(MAX_CACHE_ENTRIES, cache.size(CachePeekMode.ONHEAP));
        }
        finally {
            stopGrid();
        }
    }
}
