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
package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractRemoveFailureTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Failover test for cache remove operations when BaselineTopology is changed down
 * (existing node gets stopped and removed from BaselineTopology).
 */
public class IgniteChangingBaselineDownCacheRemoveFailoverTest extends GridCacheAbstractRemoveFailureTest {
    /** */
    private static final int GRIDS_COUNT = 5;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearCache() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(200L * 1024 * 1024)
                    .setMaxSize(200L * 1024 * 1024)
                    .setCheckpointPageBufferSize(200L * 1024 * 1024)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        startGrids(GRIDS_COUNT);

        startGrid(GRIDS_COUNT);

        grid(0).cluster().baselineAutoAdjustEnabled(false);
        grid(0).active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture createAndRunConcurrentAction(final AtomicBoolean stop, final AtomicReference<CyclicBarrier> cmp) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                U.sleep(random(5000, 10_000));

                log.info("Stopping node " + GRIDS_COUNT);

                stopGrid(GRIDS_COUNT);

                IgniteEx ig0 = grid(0);

                ig0.cluster().setBaselineTopology(baselineNodes(ig0.cluster().forServers().nodes()));

                while (!stop.get()) {
                    CyclicBarrier barrier = cmp.get();

                    if (barrier != null) {
                        log.info("Wait data check.");

                        barrier.await(60_000, TimeUnit.MILLISECONDS);

                        log.info("Finished wait data check.");
                    }
                }

                return null;
            }
        });
    }

    /** */
    private Collection<BaselineNode> baselineNodes(Collection<ClusterNode> clNodes) {
        Collection<BaselineNode> res = new ArrayList<>(clNodes.size());

        for (ClusterNode clN : clNodes)
            res.add(clN);

        return res;
    }
}
