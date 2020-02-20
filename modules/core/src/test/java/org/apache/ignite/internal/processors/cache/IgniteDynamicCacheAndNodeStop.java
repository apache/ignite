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

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class IgniteDynamicCacheAndNodeStop extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean testSpi;

    /** */
    private boolean persistent;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (testSpi) {
            TestRecordingCommunicationSpi spi = new TestRecordingCommunicationSpi();

            cfg.setCommunicationSpi(spi);
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration();
        memCfg.setPageSize(4 * 1024);
        memCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(200L * 1024 * 1024)
            .setPersistenceEnabled(persistent)
            .setName("dfltRgn"));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAndNodeStop() throws Exception {
        final Ignite ignite = startGrid(0);

        for (int i = 0; i < 3; i++) {
            log.info("Iteration: " + i);

            startGrid(1);

            final CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ignite.createCache(ccfg);

            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    ignite.destroyCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            });

            IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    barrier.await();

                    stopGrid(1);

                    return null;
                }
            });

            fut1.get();
            fut2.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeDeactivationWithConcCacheStartWithPers() throws Exception {
        runNodeDeactivationWithConcCacheStart(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeDeactivationWithConcCacheStartWithoutPers() throws Exception {
        runNodeDeactivationWithConcCacheStart(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void runNodeDeactivationWithConcCacheStart(boolean persistent) throws Exception {
        this.persistent = persistent;

        testSpi = true;

        final int nodes = 2;

        final int partsSize = 2048;

        AtomicReference<Exception> ex = new AtomicReference<>(null);

        IgniteEx crd = startGrid(0);

        final IgniteEx second = startGrid(1);

        crd.cluster().active(true);

        final AffinityTopologyVersion curTopVer = crd.context().discovery().topologyVersionEx();

        AffinityTopologyVersion cacheStartTopVer = new AffinityTopologyVersion(
            curTopVer.topologyVersion() + nodes, 1
        );

        CountDownLatch singleMsgLatch = new CountDownLatch(1);

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            blockExchangeSingleMessage(spi, cacheStartTopVer, singleMsgLatch);
        }

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() ->
            {
                try {
                    long topVer0 = curTopVer.topologyVersion();

                    startGridsMultiThreaded(2, nodes);

                    IgniteCache<Object, Object> cache = second.getOrCreateCache(new CacheConfiguration<>("myCache")
                        .setAffinity(new RendezvousAffinityFunction(false, partsSize))
                        .setBackups(1));

                    for (int i = 0; i < partsSize * 2; ++i)
                        cache.put(i, i);

                    if (persistent)
                        forceCheckpoint(second);
                }
                catch (Exception e) {
                    ex.compareAndSet(null, e);

                    e.printStackTrace();
                }
            }
        );

        singleMsgLatch.await();

        for (int gridIdx = 0; gridIdx < nodes; gridIdx++) {
            TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(gridIdx));

            spi.stopBlock();
        }

        crd.close();

        fut.get();

        assertNull(ex.get() != null ? ex.get().toString() : "", ex.get());
    }

    /**
     * @param spi SPI.
     * @param topVer Exchange topology version.
     */
    private void blockExchangeSingleMessage(
        TestRecordingCommunicationSpi spi,
        final AffinityTopologyVersion topVer,
        CountDownLatch sign) {
        spi.blockMessages((clusterNode, msg) -> {
            if (msg instanceof GridDhtPartitionsSingleMessage) {
                GridDhtPartitionsSingleMessage pMsg = (GridDhtPartitionsSingleMessage)msg;

                if (pMsg.exchangeId() != null && pMsg.exchangeId().topologyVersion().equals(topVer)) {
                    sign.countDown();

                    return true;
                }
            }

            return false;
        });
    }
}
