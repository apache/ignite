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
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * TTL manager self test.
 */
public class GridCacheTtlManagerEvictionTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test cache mode. */
    protected CacheMode cacheMode;
    private CacheMemoryMode cacheMemoryMode;
    public static final int ENTRIES_COUNT = 10_100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(cacheMemoryMode);
        ccfg.setEagerTtl(true);
        ccfg.setSwapEnabled(false);
        ccfg.setEvictionPolicy(new FifoEvictionPolicy(1000, 100));
        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.HOURS, 12)));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalEviction() throws Exception {
        checkEviction(LOCAL, CacheMemoryMode.ONHEAP_TIERED);
        checkEviction(LOCAL, CacheMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedEviction() throws Exception {
        checkEviction(PARTITIONED, CacheMemoryMode.ONHEAP_TIERED);
        checkEviction(PARTITIONED, CacheMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedEviction() throws Exception {
        checkEviction(REPLICATED, CacheMemoryMode.ONHEAP_TIERED);
        checkEviction(REPLICATED, CacheMemoryMode.OFFHEAP_TIERED);
    }

    /**
     * @param mode Cache mode.
     * @throws Exception If failed.
     */
    private void checkEviction(CacheMode mode, CacheMemoryMode memoryMode) throws Exception {
        cacheMode = mode;
        cacheMemoryMode = memoryMode;

        final IgniteKernal g = (IgniteKernal)startGrid(0);

        try {
            final IgniteCache<Object, Object> cache = g.cache(null);

            GridCacheContext<Object, Object> cctx = g.cachex().context();

            for (int i = 1; i <= ENTRIES_COUNT; i++) {
                String key = "Some test entry key#" + i;
                String value = "Some test entry value#" + i;

                cache.put(key, value);
            }

//            cctx.ttl().printMemoryStats();

            final String firstKey = "Some test entry key#0";
            final String lastKey = "Some test entry key#" + ENTRIES_COUNT;

            if (cctx.isSwapOrOffheapEnabled()) {
                assertTrue("last key should NOT be evicted", cache.containsKey(lastKey));

                assertEquals(ENTRIES_COUNT, cctx.ttl().pendingSize());
            }
            else {
                assertFalse("first key should be evicted", cache.containsKey(firstKey));

                assertTrue("last key should NOT be evicted", cache.containsKey(lastKey));

                assertEquals("Ttl Manager should NOT track evicted entries",1000, cctx.ttl().pendingSize());
            }

//            cctx.ttl().printMemoryStats();
        }
        finally {
            {
                Ignition.stopAll(true);
            }
        }
    }
}