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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounterTrackingImpl;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests the rebalancing not happens on node join for partitions belonging to coordinator even if counters are different.
 */
public class IgnitePdsSpuriousRebalancingOnNodeJoinTest extends GridCommonAbstractTest {
    /** */
    private static final int PARTS = 64;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();
        dsCfg.setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.setPageSize(1024);

        dsCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration().
            setInitialSize(100L * 1024 * 1024).
            setMaxSize(200L * 1024 * 1024).
            setPersistenceEnabled(true));

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setAtomicityMode(TRANSACTIONAL).
            setCacheMode(REPLICATED).
            setAffinity(new RendezvousAffinityFunction(false, PARTS)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testNoSpuriousRebalancing() throws Exception {
        try {
            IgniteEx crd = startGrids(2);

            crd.cluster().active(true);
            crd.cluster().baselineAutoAdjustEnabled(false);

            List<Integer> moving = movingKeysAfterJoin(crd, DEFAULT_CACHE_NAME, 10);

            int[] primParts = crd.affinity(DEFAULT_CACHE_NAME).primaryPartitions(crd.localNode());

            Arrays.sort(primParts);

            int primChangePartId = -1; // This partition will be new primary on joining node.

            for (int id : moving) {
                if (Arrays.binarySearch(primParts, id) >= 0) {
                    primChangePartId = id;

                    break;
                }
            }

            assertTrue(primChangePartId != -1);

            startGrid(2);

            resetBaselineTopology(); // Trigger partition movement.

            awaitPartitionMapExchange();

            GridCacheContext<Object, Object> ctx = crd.cachex(DEFAULT_CACHE_NAME).context();
            AffinityAssignment a0 = ctx.affinity().assignment(new AffinityTopologyVersion(3, 1));

            List<ClusterNode> nodes = a0.get(primChangePartId);

            assertEquals(3, nodes.size());

            assertEquals(crd.configuration().getConsistentId(), nodes.get(0).consistentId());

            awaitPartitionMapExchange();

            for (int k = 0; k < PARTS * 2; k++)
                crd.cache(DEFAULT_CACHE_NAME).put(k, k);

            forceCheckpoint();

            stopGrid(2);

            // Forge the counter on coordinator for switching partition.
            GridDhtLocalPartition part = ctx.topology().localPartition(primChangePartId);

            assertNotNull(part);

            PartitionUpdateCounterTrackingImpl cntr0 = (PartitionUpdateCounterTrackingImpl)part.dataStore().partUpdateCounter();

            AtomicLong cntr = U.field(cntr0, "cntr");

            cntr.set(cntr.get() - 1);

            TestRecordingCommunicationSpi.spi(crd).record((node, msg) -> msg instanceof GridDhtPartitionDemandMessage);

            startGrid(2);

            awaitPartitionMapExchange();

            // Expecting no rebalancing.
            List<Object> msgs = TestRecordingCommunicationSpi.spi(crd).recordedMessages(true);

            assertTrue("Rebalancing is not expected " + msgs, msgs.isEmpty());
        }
        finally {
            stopAllGrids();
        }
    }
}
