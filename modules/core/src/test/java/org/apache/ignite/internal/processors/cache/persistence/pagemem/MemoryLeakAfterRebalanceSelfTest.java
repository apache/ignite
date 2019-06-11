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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
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
    private final CountDownLatch preloadStartLatch = new CountDownLatch(1);

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

        boolean isSupplierNode = getTestIgniteInstanceIndex(igniteInstanceName) == 0;

        cfg.setCommunicationSpi(new TestCommunicationSpi(isSupplierNode ? preloadStartLatch : null));

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
        Ignite node = startGrid(0);

        node.cluster().active(true);

        node.cluster().baselineAutoAdjustTimeout(0);

        // Load data.
        node.cache(DEFAULT_CACHE_NAME).putAll(data);

        // Start 2 node.
        IgniteEx node2 = startGrid(1);

        IgniteInternalCache<Object, Object> cache = node2.cachex(DEFAULT_CACHE_NAME);

        // Simulate concurrent updates when preloading.
        for (int i = 0; i < size; i += 10) {
            String val = i + " v.2";

            cache.put(i, val);
            data.put(i, val);

            // Start preloading.
            if (i > size / 2)
                preloadStartLatch.countDown();
        }

        awaitPartitionMapExchange();

        // Stop node 1.
        stopGrid(0);

        awaitPartitionMapExchange();

        assertEquals(data.size(), cache.size());

        GridCacheContext cctx = cache.context();

        // Ensure that there are no duplicate entries left on data pages in page memory.
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

    /** */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private final CountDownLatch delaySupplyMessagesLatch;

        /** */
        private TestCommunicationSpi(CountDownLatch delaySupplyMessagesLatch) {
            this.delaySupplyMessagesLatch = delaySupplyMessagesLatch;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(
            final ClusterNode node,
            final Message msg,
            final IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {
            try {
                boolean supplyMsg = msg instanceof GridIoMessage &&
                    ((GridIoMessage)msg).message() instanceof GridDhtPartitionSupplyMessage;

                if (supplyMsg && delaySupplyMessagesLatch != null)
                    U.await(delaySupplyMessagesLatch, 10, TimeUnit.SECONDS);

                super.sendMessage(node, msg, ackC);
            } catch (IgniteInterruptedCheckedException e) {
                throw new IgniteSpiException(e);
            }
        }
    }
}
