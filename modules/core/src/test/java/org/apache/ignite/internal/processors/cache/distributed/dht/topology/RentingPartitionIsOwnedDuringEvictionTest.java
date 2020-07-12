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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Tests if currently evicting partition is eventually moved to OWNING state after last supplier has left.
 */
public class RentingPartitionIsOwnedDuringEvictionTest extends GridCommonAbstractTest {
    /** */
    private boolean persistence;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        if (persistence) {
            DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
            dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(persistence);
            cfg.setDataStorageConfiguration(dsCfg);
        }

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
    @Test
    public void testOwnedAfterEviction() throws Exception {
        testOwnedAfterEviction(false);
    }

    /** */
    @Test
    public void testOwnedAfterEvictionWithPersistence() throws Exception {
        testOwnedAfterEviction(true);
    }

    /**
     * @throws Exception If failed.
     */
    private void testOwnedAfterEviction(boolean persistence) throws Exception {
        this.persistence = persistence;

        try {
            IgniteEx g0 = startGrid(0);

            if (persistence)
                g0.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = g0.getOrCreateCache(DEFAULT_CACHE_NAME);

            int p0 = movingKeysAfterJoin(g0, DEFAULT_CACHE_NAME, 1).get(0);

            List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, 20_000, 0);

            try (IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (Integer key : keys)
                    ds.addData(key, key);
            }

            IgniteEx g1 = startGrid(1);

            if (persistence)
                resetBaselineTopology();

            awaitPartitionMapExchange();

            GridDhtPartitionState state = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0).state();

            assertTrue(state == RENTING || state == EVICTED);

            g1.close();

            if (persistence)
                resetBaselineTopology();

            GridDhtTopologyFuture fut = g0.cachex(DEFAULT_CACHE_NAME).context().topology().topologyVersionFuture();

            assertEquals(3, fut.initialVersion().topologyVersion());

            fut.get();

            GridDhtLocalPartition part = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

            assertEquals(OWNING, part.state());
        }
        finally {
            stopAllGrids();
        }
    }
}
