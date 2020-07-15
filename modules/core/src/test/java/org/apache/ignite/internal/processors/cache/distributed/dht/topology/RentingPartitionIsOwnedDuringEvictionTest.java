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
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
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
        testOwnedAfterEviction(false, 0);
    }

    /** */
    @Test
    public void testOwnedAfterEvictionWithPersistence() throws Exception {
        testOwnedAfterEviction(true, 0);
    }

    /** */
    @Test
    public void testOwnedAfterEviction_2() throws Exception {
        testOwnedAfterEviction(false, 1);
    }

    /** */
    @Test
    public void testOwnedAfterEvictionWithPersistence_2() throws Exception {
        testOwnedAfterEviction(true, 1);
    }

    /**
     * @param persistence Persistence.
     * @param backups Backups.
     *
     * @throws Exception If failed.
     */
    private void testOwnedAfterEviction(boolean persistence, int backups) throws Exception {
        this.persistence = persistence;

        try {
            IgniteEx g0 = startGrids(backups + 1);

            if (persistence)
                g0.cluster().state(ClusterState.ACTIVE);

            awaitPartitionMapExchange();

            IgniteCache<Object, Object> cache = g0.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME).
                setCacheMode(CacheMode.PARTITIONED).
                setBackups(backups).
                setAffinity(new RendezvousAffinityFunction(false, 64)));

            int p0 = evictingPartitionsAfterJoin(g0, cache, 1).get(0);

            final int cnt = 50_000;
            final int cnt2 = backups == 0 && persistence ? 0 : 1_000; // Handle partition loss.

            List<Integer> keys = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, cnt, 0);
            List<Integer> keys2 = partitionKeys(g0.cache(DEFAULT_CACHE_NAME), p0, cnt2, cnt);

            try (IgniteDataStreamer<Object, Object> ds = g0.dataStreamer(DEFAULT_CACHE_NAME)) {
                for (Integer key : keys)
                    ds.addData(key, key);
            }

            IgniteEx joining = startGrid(backups + 1);

            if (persistence)
                resetBaselineTopology();

            GridDhtLocalPartition evicting = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return evicting.state() == RENTING;
                }
            }, 5_000));

            GridDhtPartitionState state = evicting.state();

            assertTrue(state.toString(), state == RENTING || state == EVICTED);

            // These updates should not be lost.
            IgniteInternalFuture fut = GridTestUtils.runAsync(new Runnable() {
                @Override public void run() {
                    Random r = new Random();

                    for (Integer k : keys2)
                        grid(r.nextInt(backups + 1)).cache(DEFAULT_CACHE_NAME).put(k, k);
                }
            });

            joining.close();

            if (persistence)
                resetBaselineTopology();

            awaitPartitionMapExchange(true, true, null);

            GridDhtLocalPartition part = g0.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(p0);

            assertEquals(backups == 0 && persistence ? LOST : OWNING, part.state());

            fut.get();

            assertPartitionsSame(idleVerify(g0, DEFAULT_CACHE_NAME));

            if (backups > 0)
                assertEquals(cnt + cnt2, part.dataStore().fullSize());
        }
        finally {
            stopAllGrids();
        }
    }
}
