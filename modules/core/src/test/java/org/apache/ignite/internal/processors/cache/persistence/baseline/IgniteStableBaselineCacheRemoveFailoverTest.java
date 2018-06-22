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

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractRemoveFailureTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Failover cache test with remove operations executed with presence of BaselineTopology
 * when one random node from BLT is constantly restarted during the load.
 */
public class IgniteStableBaselineCacheRemoveFailoverTest extends GridCacheAbstractRemoveFailureTest {
    /** */
    private static final int GRIDS_COUNT = 3;

    /** */
    private static final int OUT_OF_BASELINE_GRID_ID = GRIDS_COUNT;

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
                    .setInitialSize(512L * 1024 * 1024)
                    .setMaxSize(512L * 1024 * 1024)
                    .setCheckpointPageBufferSize(512L * 1024 * 1024)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        startGrids(GRIDS_COUNT);

        grid(0).active(true);

        startGrid(OUT_OF_BASELINE_GRID_ID);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteInternalFuture createAndRunConcurrentAction(final AtomicBoolean stop, final AtomicReference<CyclicBarrier> cmp) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                Thread.currentThread().setName("restart-thread");

                while (!stop.get()) {
                    U.sleep(random(1000, 3000));

                    if (ThreadLocalRandom.current().nextInt(5) < 3)
                        killAndRestart(stop, random(1, GRIDS_COUNT + 1));
                    else
                        killAndRestart(stop, OUT_OF_BASELINE_GRID_ID);

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
}
