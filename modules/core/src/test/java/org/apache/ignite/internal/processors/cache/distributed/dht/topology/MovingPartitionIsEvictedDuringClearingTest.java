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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.resource.DependencyResolver;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Tests a scenario when a clearing partition is attempted to evict after a call to
 * {@link GridDhtPartitionTopology#tryFinishEviction(GridDhtLocalPartition)}.
 *
 * Such a scenario can leave a partition in RENTING state until the next exchange. It's actually acceptable behavior.
 */
@WithSystemProperty(key = "IGNITE_PRELOAD_RESEND_TIMEOUT", value = "0")
public class MovingPartitionIsEvictedDuringClearingTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Need at least 2 threads in pool to avoid deadlock on clearing.
        cfg.setRebalanceThreadPoolSize(ThreadLocalRandom.current().nextInt(3) + 2);
        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration().setWalSegmentSize(4 * 1024 * 1024);
        dsCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(200 * 1024 * 1024);
        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME).
            setBackups(2).
            setAffinity(new RendezvousAffinityFunction(false, 64)));

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

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMovingToEvicted() throws Exception {
        IgniteEx crd = startGrids(3);

        crd.cluster().state(ClusterState.ACTIVE);

        final int evictingPart = evictingPartitionsAfterJoin(grid(2), grid(2).cache(DEFAULT_CACHE_NAME), 1).get(0);

        final int cnt = 1_100;
        final int delta = 2_000;
        final int rmv = 1_500;

        loadDataToPartition(evictingPart, getTestIgniteInstanceName(0), DEFAULT_CACHE_NAME, cnt, 0, 3);

        forceCheckpoint();

        stopGrid(2);

        loadDataToPartition(evictingPart, getTestIgniteInstanceName(0), DEFAULT_CACHE_NAME, delta, cnt, 3);

        // Removal required for triggering full rebalancing.
        List<Integer> clearKeys = partitionKeys(grid(0).cache(DEFAULT_CACHE_NAME), evictingPart, rmv, cnt);

        for (Integer clearKey : clearKeys)
            grid(0).cache(DEFAULT_CACHE_NAME).remove(clearKey);

        CountDownLatch lock = new CountDownLatch(1);
        CountDownLatch unlock = new CountDownLatch(1);

        // Start node and delay preloading in the middle of partition clearing.
        IgniteEx g2 = startGrid(2, new DependencyResolver() {
            @Override public <T> T resolve(T instance) {
                if (instance instanceof GridDhtPartitionTopologyImpl) {
                    GridDhtPartitionTopologyImpl top = (GridDhtPartitionTopologyImpl) instance;

                    top.partitionFactory(new GridDhtPartitionTopologyImpl.PartitionFactory() {
                        @Override public GridDhtLocalPartition create(
                            GridCacheSharedContext ctx,
                            CacheGroupContext grp,
                            int id,
                            boolean recovery
                        ) {
                            return id == evictingPart ?
                                new GridDhtLocalPartitionSyncEviction(ctx, grp, id, recovery, 2, lock, unlock) :
                                new GridDhtLocalPartition(ctx, grp, id, recovery);
                        }
                    });
                }

                return instance;
            }
        });

        assertTrue(U.await(lock, GridDhtLocalPartitionSyncEviction.TIMEOUT, TimeUnit.MILLISECONDS));

        startGrid(4);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        // Give some time for partition state messages to process.
        doSleep(3_000);

        // Finish clearing.
        unlock.countDown();

        awaitPartitionMapExchange();

        // Partition will remaing in renting state until next exchange.
        assertEquals(RENTING, g2.cachex(DEFAULT_CACHE_NAME).context().topology().localPartition(evictingPart).state());

        validadate(cnt + delta - rmv);

        stopGrid(2);
        startGrid(2);

        awaitPartitionMapExchange(true, true, null);

        validadate(cnt + delta - rmv);
    }

    /**
     * @param size Size.
     */
    private void validadate(int size) {
        assertPartitionsSame(idleVerify(grid(0), DEFAULT_CACHE_NAME));

        for (Ignite grid : G.allGrids())
            assertEquals(size, grid.cache(DEFAULT_CACHE_NAME).size());
    }
}
