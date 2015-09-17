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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MARSH_CACHE_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;

/**
 * Sanity test for cache types.
 */
public class IgniteInternalCacheTypesTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (gridName.equals(getTestGridName(0))) {
            CacheConfiguration ccfg = defaultCacheConfiguration();

            ccfg.setName(CACHE1);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheTypes() throws Exception {
        Ignite ignite0 = startGrid(0);

        checkCacheTypes(ignite0, CACHE1);

        Ignite ignite1 = startGrid(1);

        checkCacheTypes(ignite1, CACHE1);

        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setName(CACHE2);

        assertNotNull(ignite0.createCache(ccfg));

        checkCacheTypes(ignite0, CACHE1, CACHE2);
        checkCacheTypes(ignite1, CACHE1, CACHE2);

        Ignite ignite2 = startGrid(2);

        checkCacheTypes(ignite0, CACHE1, CACHE2);
        checkCacheTypes(ignite1, CACHE1, CACHE2);
        checkCacheTypes(ignite2, CACHE1, CACHE2);
    }

    /**
     * @param ignite Ignite.
     * @param userCaches User caches.
     */
    private void checkCacheTypes(final Ignite ignite, String... userCaches) {
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache(CU.UTILITY_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache(CU.MARSH_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite.cache(CU.ATOMICS_CACHE_NAME);

                return null;
            }
        }, IllegalStateException.class, null);

        checkCache(ignite, CU.UTILITY_CACHE_NAME, UTILITY_CACHE_POOL, false, true);

        checkCache(ignite, CU.MARSH_CACHE_NAME, MARSH_CACHE_POOL, false, false);

        checkCache(ignite, CU.ATOMICS_CACHE_NAME, SYSTEM_POOL, false, true);

        for (String cache : userCaches)
            checkCache(ignite, cache, SYSTEM_POOL, true, false);
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param plc Expected IO policy.
     * @param user Expected user cache flag.
     * @param sysTx Expected system transaction flag.
     */
    private void checkCache(
        Ignite ignite,
        String name,
        byte plc,
        boolean user,
        boolean sysTx) {
        GridCacheAdapter cache = ((IgniteKernal)ignite).context().cache().internalCache(name);

        assertNotNull("No cache " + name, cache);
        assertEquals("Unexpected property for cache: " + cache.name(), plc, cache.context().ioPolicy());
        assertEquals("Unexpected property for cache: " + cache.name(), user, cache.context().userCache());
        assertEquals("Unexpected property for cache: " + cache.name(), sysTx, cache.context().systemTx());
    }
}
