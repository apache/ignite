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
package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.mxbean.CacheGroupMetricsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * A test class for any tests that check rebalancing process in presence of client nodes and
 * IGNITE_DISABLE_WAL_DURING_REBALANCING optimization enabled.
 */
public class IgnitePdsCacheRebalancingWithClientsTest extends GridCommonAbstractTest {
    /** Block message predicate to set to Communication SPI in node configuration. */
    private IgniteBiPredicate<ClusterNode, Message> blockMessagePredicate;

    /** */
    private static final int CACHE1_PARTS_NUM = 8;

    /** */
    private static final int CACHE2_PARTS_NUM = 16;

    /** */
    private static final int CACHE3_PARTS_NUM = 32;

    /** */
    private static final String CACHE3_NAME = "cache3";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg1 = new CacheConfiguration("cache1")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE1_PARTS_NUM));

        CacheConfiguration ccfg2 = new CacheConfiguration("cache2")
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE2_PARTS_NUM));

        CacheConfiguration ccfg3 = new CacheConfiguration(CACHE3_NAME)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAffinity(new RendezvousAffinityFunction(false, CACHE3_PARTS_NUM));

        cfg.setCacheConfiguration(ccfg1, ccfg2, ccfg3);

        if ("client".equals(igniteInstanceName))
            cfg.setClientMode(true);
        else {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration()
                .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(256 * 1024 * 1024));

            cfg.setDataStorageConfiguration(dsCfg);
        }

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();
        commSpi.blockMessages(blockMessagePredicate);

        cfg.setCommunicationSpi(commSpi);


        return cfg;
    }

    /**
     * Verifies that if client joins topology during rebalancing process, rebalancing finishes successfully,
     * all partitions are owned as expected upon completion rebalancing.
     */
    public void testClientJoinsDuringRebalancing() throws Exception {
        Ignite ig0 = startGrids(2);

        ig0.active(true);

        for (int i = 0; i < 3; i++)
            fillCache(ig0.getOrCreateCache("cache" + i));

        String ig1Name = "node01-" + grid(1).localNode().consistentId();

        stopGrid(1);

        cleanPersistenceFiles(ig1Name);

        int groupId = ((IgniteEx) ig0).cachex(CACHE3_NAME).context().groupId();

        blockMessagePredicate = (node, msg) -> {
            if (msg instanceof GridDhtPartitionDemandMessage)
                return ((GridDhtPartitionDemandMessage) msg).groupId() == groupId;

            return false;
        };

        IgniteEx ig1 = startGrid(1);

        startGrid("client");

        stopGrid("client");

        CacheGroupMetricsMXBean mxBean = ig1.cachex(CACHE3_NAME).context().group().mxBean();

        assertTrue("Unexpected moving partitions count: " + mxBean.getLocalNodeMovingPartitionsCount(),
            mxBean.getLocalNodeMovingPartitionsCount() == CACHE3_PARTS_NUM);

        TestRecordingCommunicationSpi commSpi = (TestRecordingCommunicationSpi) ig1
            .configuration().getCommunicationSpi();

        commSpi.stopBlock();

        boolean waitResult = GridTestUtils.waitForCondition(
            () -> mxBean.getLocalNodeMovingPartitionsCount() == 0,
            30_000);

        assertTrue("Failed to wait for owning all partitions, parts in moving state: "
            + mxBean.getLocalNodeMovingPartitionsCount(), waitResult);
    }

    private void cleanPersistenceFiles(String igName) throws Exception {
        String ig1DbPath = Paths.get(DFLT_STORE_DIR, igName).toString();

        File igDbDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), ig1DbPath, false);

        U.delete(igDbDir);
        Files.createDirectory(igDbDir.toPath());

        String ig1DbWalPath = Paths.get(DFLT_STORE_DIR, "wal", igName).toString();

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), ig1DbWalPath, false));
    }

    private void fillCache(IgniteCache cache) {
        for (int i = 0; i < 2_000; i++)
            cache.put(i, "value_" + i);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        System.setProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_DISABLE_WAL_DURING_REBALANCING);
    }
}
