/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Additional tests for the WAL disabling during rebalance optimization.
 */
public class IgniteDisableWalOnRebalanceTest extends GridCommonAbstractTest {
    /** Number of partitions in the test. */
    private static final int PARTITION_COUNT = 32;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
            )
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(
                defaultCacheConfiguration()
                    .setBackups(1)
                    .setAffinity(new RendezvousAffinityFunction(false, PARTITION_COUNT))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests a case when WAL for a cache gets disabled during rebalance because a node has no owning partitions, and
     * then the node is shut down. This should not prevent the node from successfully starting up.
     */
    @Test
    public void testDisabledWalOnRebalance() throws Exception {
        IgniteEx ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        int entryCnt = PARTITION_COUNT * 200;

        int val = 0;

        for (int k = 0; k < entryCnt; k++)
            cache.put(k, val++);

        stopGrid(1, false);

        // Need to update all partitions in order to enable rebalance optimization (disabling WAL while rebalancing).
        for (int k = 0; k < entryCnt; k++)
            cache.put(k, val++);

        // Block rebalancing process for the specified cache.
        IgniteEx restartedNode = startGrid(1, (IgniteConfiguration cfg) ->
            ((TestRecordingCommunicationSpi)cfg.getCommunicationSpi()).blockMessages((node, msg) -> {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                    return msg0.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
                }

                return false;
            }));

        TestRecordingCommunicationSpi demanderSpi = TestRecordingCommunicationSpi.spi(restartedNode);

        // Wait for the rebalance to start.
        demanderSpi.waitForBlocked();

        assertFalse(restartedNode.cachex(DEFAULT_CACHE_NAME).context().group().walEnabled());

        // Stop the node and skip the checkpoint.
        stopGrid(1, true);

        restartedNode = startGrid(1);

        awaitPartitionMapExchange();

        assertTrue(restartedNode.cachex(DEFAULT_CACHE_NAME).context().group().walEnabled());
    }
}
