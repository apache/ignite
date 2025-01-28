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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.File;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests a fall back from historical to full rebalance if WAL had been corrupted after it was reserved.
 */
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, value = "0")
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_DISABLE_GRP_STATE_LAZY_STORE, value = "true")
public class CacheRebalanceWithRemovedWalSegment extends GridCommonAbstractTest {
    /** Listening logger. */
    private ListeningTestLogger listeningLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(listeningLog)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setWalSegmentSize(512 * 1024)
                .setWalSegments(2)
                .setCheckpointFrequency(600_000)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setMaxSize(200L * 1024 * 1024)
                    .setPersistenceEnabled(true)))
            .setConsistentId(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setBackups(1)
                .setAffinity(new RendezvousAffinityFunction(false, 16)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog = new ListeningTestLogger(log);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        byte[] testVal = new byte[20 * 1024];

        for (int i = 0; i < 300; i++)
            cache.put(i, testVal);

        forceCheckpoint();

        ignite(1).close();

        for (int i = 300; i < 500; i++)
            cache.put(i, testVal);

        forceCheckpoint();

        stopAllGrids();

        ignite = startGridWithBlockedDemandMessages(1, 0);
        startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        GridDhtPartitionsExchangeFuture exchangeFut = ignite(0).context().cache().context().exchange().lastTopologyFuture();

        // Waiting for reservation, otherwise we can catch a problem during reservation.
        exchangeFut.get();

        TestRecordingCommunicationSpi.spi(ignite).waitForBlocked();

        File walPath = ignite(0).context().pdsFolderResolver().resolveDirectories().walArchive();

        for (File file : walPath.listFiles()) {
            if (U.delete(file))
                info("File deleted " + file);
            else
                info("Can't delete file " + file);
        }

        LogListener lsnr = LogListener.matches("Failed to continue supplying [grp=" + DEFAULT_CACHE_NAME
            + ", demander=" + ignite.localNode().id()
            + ", topVer=" + exchangeFut.topologyVersion() + ']').build();

        listeningLog.registerListener(lsnr);

        TestRecordingCommunicationSpi.spi(ignite).stopBlock();

        awaitPartitionMapExchange();

        assertTrue(lsnr.check());

        assertPartitionsSame(idleVerify(ignite, DEFAULT_CACHE_NAME));
    }

    /**
     * Starts a node and blocks demand message sending to other one.
     *
     * @param nodeIdx Start node index.
     * @param demandNodeIdx Demand node index.
     * @return Started node Ignite instance.
     * @throws Exception If failed.
     */
    private IgniteEx startGridWithBlockedDemandMessages(int nodeIdx, int demandNodeIdx) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(nodeIdx));

        TestRecordingCommunicationSpi spi = (TestRecordingCommunicationSpi)cfg.getCommunicationSpi();

        spi.blockMessages(GridDhtPartitionDemandMessage.class, getTestIgniteInstanceName(demandNodeIdx));

        return startGrid(cfg);
    }
}
