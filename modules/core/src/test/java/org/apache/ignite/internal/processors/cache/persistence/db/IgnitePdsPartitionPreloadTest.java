/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test partition preload for varios cache modes.
 */
public class IgnitePdsPartitionPreloadTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Test entry count. */
    public static final int ENTRY_CNT = 1_000;

    /** Grid count. */
    private static final int GRIDS_CNT = 2;

    /** */
    public static final String CACHE_1 = "cache1";

    /** */
    public static final String CACHE_2 = "cache2";

    /** */
    private static final String CLIENT_GRID_NAME = "client";

    /** */
    public static final int PRELOAD_PARTITION_ID = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(CLIENT_GRID_NAME.equals(gridName));

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(150L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(1024)
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<Integer, byte[]> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        startGridsMultiThreaded(GRIDS_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        Ignite crd = startGridsMultiThreaded(GRIDS_CNT);

        int partId = PRELOAD_PARTITION_ID;
        int cnt = 0;
        int i = 0;

        // Load test data only once for all tests to speedup things.
        try (IgniteDataStreamer<Integer, byte[]> streamer = crd.dataStreamer(CACHE_1)) {
            while(cnt < ENTRY_CNT) {
                if (crd.affinity(CACHE_1).partition(i) == partId) {
                    streamer.addData(i, new byte[1024 * 2 / 3]);

                    cnt++;
                }

                i++;
            }
        }

        for (i = 0; i < ENTRY_CNT; i++)
            crd.cache(CACHE_2).put(i, new byte[1024 * 2 / 3]);

        forceCheckpoint();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    public void testPreloadPartition() throws Exception {
        try {
            startGridsMultiThreaded(GRIDS_CNT);

            Ignite testNode = startGrid("client");

            doTestPreloadPartition(testNode);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void doTestPreloadPartition(Ignite testNode) throws Exception {
        testNode.cache(DEFAULT_CACHE_NAME).preloadPartition(PRELOAD_PARTITION_ID);
    }
}
