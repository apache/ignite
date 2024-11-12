/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;

/**
 *
 */
public class CheckpointListenerForRegionTest extends GridCommonAbstractTest {
    /** This number show how many mandatory methods will be called on checkpoint listener during checkpoint. */
    private static final int CALLS_COUNT_PER_CHECKPOINT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setCheckpointFrequency(100_000);
        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(300L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 16)));

        return cfg;
    }

    /**
     * 1. Start the one node.
     * 2. Configure the default cache.
     * 3. Set the checkpoint listeners(for default region and for all regions) to watch the checkpoint.
     * 4. Fill the data and trigger the checkpoint.
     * 5. Expected: Both listeners should be called.
     * 6. Remove the default region from the checkpoint.
     * 7. Fill the data and trigger the checkpoint.
     * 8. Expected: The only listener for all regions should be called.
     * 9. Return default region back to the checkpoint.
     * 10. Fill the data and trigger the checkpoint.
     * 11. Expected: Both listeners should be called.
     *
     * @throws Exception if fail.
     */
    @Test
    public void testCheckpointListenersInvokedOnlyIfRegionConfigured() throws Exception {
        //given: One started node with default cache.
        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Object> cache = ignite0.cache(DEFAULT_CACHE_NAME);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)(ignite0.context().cache().context().database());

        DataRegion dfltRegion = db.checkpointedDataRegions().stream()
            .filter(region -> DFLT_DATA_REG_DEFAULT_NAME.equals(region.config().getName()))
            .findFirst()
            .orElse(null);

        assertNotNull("Expected default data region in checkpoint list is not found.", dfltRegion);

        //and: Configure the listeners(for default region and for all regions) for watching for checkpoint.
        AtomicInteger checkpointListenerDfltRegionCounter = checkpointListenerWatcher(db, dfltRegion);
        AtomicInteger checkpointListenerAllRegionCounter = checkpointListenerWatcher(db, null);

        //when: Checkpoint happened.
        fillDataAndCheckpoint(ignite0, cache);

        //then: Both listeners should be called.
        assertEquals(CALLS_COUNT_PER_CHECKPOINT, checkpointListenerDfltRegionCounter.get());
        assertEquals(CALLS_COUNT_PER_CHECKPOINT, checkpointListenerAllRegionCounter.get());

        //Remove the default region from checkpoint.
        db.checkpointedDataRegions().remove(dfltRegion);

        //when: Checkpoint happened.
        fillDataAndCheckpoint(ignite0, cache);

        //then: Only listener for all regions should be called.
        assertEquals(CALLS_COUNT_PER_CHECKPOINT, checkpointListenerDfltRegionCounter.get());
        assertEquals(2 * CALLS_COUNT_PER_CHECKPOINT, checkpointListenerAllRegionCounter.get());

        assertTrue(
            "Expected default data region in all regions list is not found.",
            db.dataRegions().stream().anyMatch(region -> DFLT_DATA_REG_DEFAULT_NAME.equals(region.config().getName()))
        );

        //Return default region back to the checkpoint.
        db.checkpointedDataRegions().add(dfltRegion);

        //when: Checkpoint happened.
        fillDataAndCheckpoint(ignite0, cache);

        //then: Both listeners should be called.
        assertEquals(2 * CALLS_COUNT_PER_CHECKPOINT, checkpointListenerDfltRegionCounter.get());
        assertEquals(3 * CALLS_COUNT_PER_CHECKPOINT, checkpointListenerAllRegionCounter.get());
    }

    /**
     * Fill the data and trigger the checkpoint after that.
     */
    private void fillDataAndCheckpoint(
        IgniteEx ignite0,
        IgniteCache<Integer, Object> cache
    ) throws IgniteCheckedException {
        for (int j = 0; j < 1024; j++)
            cache.put(j, j);

        forceCheckpoint(ignite0);
    }

    /**
     * Add checkpoint listener which count the number of listener calls during each checkpoint.
     *
     * @param db Shared manager for manage the listeners.
     * @param defaultRegion Region for which listener should be added.
     * @return Integer which count the listener calls.
     */
    @NotNull
    private AtomicInteger checkpointListenerWatcher(GridCacheDatabaseSharedManager db, DataRegion defaultRegion) {
        AtomicInteger checkpointListenerCounter = new AtomicInteger();

        db.addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                checkpointListenerCounter.getAndIncrement();
            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                checkpointListenerCounter.getAndIncrement();
            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                checkpointListenerCounter.getAndIncrement();
            }
        }, defaultRegion);
        return checkpointListenerCounter;
    }
}
