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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Check page memory consistency after preloading.
 */
@RunWith(Parameterized.class)
public class MemoryLeakAfterRebalanceSelfTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode cacheAtomicityMode;

    /** */
    @Parameterized.Parameters(name = " [atomicity={0}]")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][] {{CacheAtomicityMode.ATOMIC}, {CacheAtomicityMode.TRANSACTIONAL}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                .setMaxSize(200 * 1024 * 1024)
                .setPersistenceEnabled(true)));

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setAtomicityMode(cacheAtomicityMode);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));

        cfg.setCacheConfiguration(ccfg);

        cfg.setRebalanceThreadPoolSize(4);

        return cfg;
    }

    /** Initialization. */
    @Before
    public void before() throws Exception {
        cleanPersistenceDir();
    }

    /** Clean up. */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPreloadingWithConcurrentUpdates() throws Exception {
        int size = GridTestUtils.SF.applyLB(500_000, 5_000);

        // Prepare data.
        Map<Integer, String> data = new HashMap<>(U.capacity(size));

        for (int i = 0; i < size; i++)
            data.put(i, i + " v.1");

        // Start 1 node.
        Ignite node0 = startGrid(0);

        node0.cluster().active(true);

        node0.cluster().baselineAutoAdjustTimeout(0);

        IgniteCache<Integer, String> cache0 = node0.cache(DEFAULT_CACHE_NAME);

        // Load data.
        cache0.putAll(data);

        TestRecordingCommunicationSpi.spi(node0)
            .blockMessages((node, msg) ->
                msg instanceof GridDhtPartitionSupplyMessage
                    && ((GridCacheGroupIdMessage)msg).groupId() == groupIdForCache(node0, DEFAULT_CACHE_NAME)
            );

        // Start 2 node.
        IgniteEx node1 = startGrid(1);

        TestRecordingCommunicationSpi.spi(node0).waitForBlocked();

        // Simulate concurrent updates when preloading.
        for (int i = 0; i < size; i += 10) {
            String val = i + " v.2";

            cache0.put(i, val);
            data.put(i, val);

            // Start preloading.
            if (i == size / 2)
                TestRecordingCommunicationSpi.spi(node0).stopBlock();
        }

        awaitPartitionMapExchange();

        // Stop node 1.
        stopGrid(0);

        awaitPartitionMapExchange();

        IgniteInternalCache<Integer, String> cache1 = node1.cachex(DEFAULT_CACHE_NAME);

        assertEquals(data.size(), cache1.size());

        GridCacheContext cctx = cache1.context();

        // Make sure that there are no duplicate entries on the data pages in the pages memory.
        try (GridCloseableIterator<Cache.Entry<Integer, String>> iter = cctx.offheap().cacheEntriesIterator(
            cctx,
            true,
            true,
            cctx.topology().readyTopologyVersion(),
            false,
            null,
            true)) {

            while (iter.hasNext()) {
                Cache.Entry<Integer, String> entry = iter.next();

                Integer key = entry.getKey();

                String exp = data.remove(key);

                assertEquals(exp, entry.getValue());
            }
        }

        assertTrue(data.isEmpty());
    }
}
