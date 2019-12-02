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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsAbstractMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridExchangeFreeSwitchTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** */
    private boolean persistence;

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private IgniteClosure<String, CacheConfiguration[]> cacheC;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setCacheConfiguration(cacheC != null ?
            cacheC.apply(igniteInstanceName) : new CacheConfiguration[] {cacheConfiguration()});

        DataStorageConfiguration cfg1 = new DataStorageConfiguration();

        DataRegionConfiguration drCfg = new DataRegionConfiguration();

        if (persistence) {
            drCfg.setPersistenceEnabled(true);

            cfg.setActiveOnStart(false);
            cfg.setAutoActivationEnabled(false);
        }

        cfg1.setDefaultDataRegionConfiguration(drCfg);

        cfg.setDataStorageConfiguration(cfg1);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAffinity(affinityFunction(null));
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(0);

        return ccfg;
    }

    /**
     * @param parts Number of partitions.
     * @return Affinity function.
     */
    protected AffinityFunction affinityFunction(@Nullable Integer parts) {
        return new RendezvousAffinityFunction(false,
            parts == null ? RendezvousAffinityFunction.DFLT_PARTITION_COUNT : parts);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Checks Partition Exchange happen in case of baseline auto-adjust (in-memory cluster).
     * It's not possible to perform switch since primaries may change.
     */
    @Test
    public void testNonBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        testNodeLeftOnFullyRebalancedCluster();
    }

    /**
     * Checks Partition Exchange is absent in case of fixed baseline.
     * It's possible to perform switch since primaries can't change.
     */
    @Test
    public void testBaselineNodeLeftOnFullyRebalancedCluster() throws Exception {
        persistence = true;

        try {
            testNodeLeftOnFullyRebalancedCluster();
        }
        finally {
            persistence = false;
        }
    }

    /**
     * Checks node left PME absent/present on fully rebalanced topology (Latest PME == LAS).
     */
    private void testNodeLeftOnFullyRebalancedCluster() throws Exception {
        int nodes = 10;

        Ignite ignite = startGridsMultiThreaded(nodes, true);

        ignite.cluster().active(true);

        AtomicLong cnt = new AtomicLong();

        for (int i = 0; i < nodes; i++) {
            TestRecordingCommunicationSpi spi =
                (TestRecordingCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            spi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    if (msg.getClass().equals(GridDhtPartitionsSingleMessage.class) &&
                        ((GridDhtPartitionsAbstractMessage)msg).exchangeId() != null)
                        cnt.incrementAndGet();

                    if (!persistence)
                        return false;

                    return msg.getClass().equals(GridDhtPartitionsSingleMessage.class) ||
                        msg.getClass().equals(GridDhtPartitionsFullMessage.class);
                }
            });
        }

        Random r = new Random();

        while (nodes > 1) {
            G.allGrids().get(r.nextInt(--nodes)).close(); // Stopping random node.

            awaitPartitionMapExchange(true, true, null, true);

            assertEquals(persistence ? 0 /*PME absent*/ : (nodes - 1) /*regular PME*/, cnt.get());

            IgniteEx alive = (IgniteEx)G.allGrids().get(0);

            assertTrue(alive.context().cache().context().exchange().lastFinishedFuture().rebalanced());

            cnt.set(0);
        }
    }
}
