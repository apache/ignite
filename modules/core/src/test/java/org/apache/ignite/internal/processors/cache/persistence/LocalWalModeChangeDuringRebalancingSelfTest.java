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

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class LocalWalModeChangeDuringRebalancingSelfTest extends GridCommonAbstractTest {
    /** */
    private static boolean disableWalDuringRebalancing = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(200 * 1024 * 1024)
                        .setInitialSize(200 * 1024 * 1024)
                )
                .setCheckpointFrequency(999_999_999_999L)
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setRebalanceDelay(-1)
        );

        cfg.setDisableWalDuringRebalancing(disableWalDuringRebalancing);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        disableWalDuringRebalancing = true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalDisabledDuringRebalancing() throws Exception {
        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        long newIgniteStartedTimestamp = System.currentTimeMillis();

        ignite.cluster().setBaselineTopology(4);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertTrue("WAL should be disabled until rebalancing is finished", !grpCtx.walEnabled());

        long rebalanceStartedTimestamp = System.currentTimeMillis();

        for (int i = 0; i < 4; i++)
            grid(i).cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue("WAL should be enabled after rebalancing is finished", grpCtx.walEnabled());

        long rebalanceFinishedTimestamp = System.currentTimeMillis();

        for (Integer k = 0; k < 1000; k++)
            assertEquals("k=" + k, k, cache.get(k));

        GridCacheDatabaseSharedManager.CheckpointHistory cpHistory =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        int checkpointsBeforeNodeStarted = 0;
        int checkpointsBeforeRebalance = 0;
        int checkpointsAfterRebalance = 0;

        for (Long timestamp : cpHistory.checkpoints()) {
            if (timestamp < newIgniteStartedTimestamp)
                checkpointsBeforeNodeStarted++;
            else if (timestamp >= newIgniteStartedTimestamp && timestamp < rebalanceStartedTimestamp)
                checkpointsBeforeRebalance++;
            else if (timestamp >= rebalanceStartedTimestamp && timestamp <= rebalanceFinishedTimestamp)
                checkpointsAfterRebalance++;
        }

        assertEquals(1, checkpointsBeforeNodeStarted); // checkpoint on start
        assertEquals(0, checkpointsBeforeRebalance); // no checkpoints before rebalance
        assertEquals(1, checkpointsAfterRebalance); // checkpoint on WAL activation
    }

    /**
     * @throws Exception If failed.
     */
    public void testWalNotDisabledIfParameterSetToFalse() throws Exception {
        disableWalDuringRebalancing = false;

        Ignite ignite = startGrids(3);

        ignite.cluster().active(true);

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int k = 0; k < 1000; k++)
            cache.put(k, k);

        IgniteEx newIgnite = startGrid(3);

        long newIgniteStartedTimestamp = System.currentTimeMillis();

        ignite.cluster().setBaselineTopology(4);

        CacheGroupContext grpCtx = newIgnite.cachex(DEFAULT_CACHE_NAME).context().group();

        assertTrue("WAL should be enabled until rebalancing is finished", grpCtx.walEnabled());

        long rebalanceStartedTimestamp = System.currentTimeMillis();

        for (int i = 0; i < 4; i++)
            grid(i).cache(DEFAULT_CACHE_NAME).rebalance();

        awaitPartitionMapExchange();

        assertTrue("WAL should be enabled after rebalancing is finished", grpCtx.walEnabled());

        long rebalanceFinishedTimestamp = System.currentTimeMillis();

        for (Integer k = 0; k < 1000; k++)
            assertEquals("k=" + k, k, cache.get(k));

        GridCacheDatabaseSharedManager.CheckpointHistory cpHistory =
            ((GridCacheDatabaseSharedManager)newIgnite.context().cache().context().database()).checkpointHistory();

        int checkpointsBeforeNodeStarted = 0;
        int checkpointsBeforeRebalance = 0;
        int checkpointsAfterRebalance = 0;

        for (Long timestamp : cpHistory.checkpoints()) {
            if (timestamp < newIgniteStartedTimestamp)
                checkpointsBeforeNodeStarted++;
            else if (timestamp >= newIgniteStartedTimestamp && timestamp < rebalanceStartedTimestamp)
                checkpointsBeforeRebalance++;
            else if (timestamp >= rebalanceStartedTimestamp && timestamp <= rebalanceFinishedTimestamp)
                checkpointsAfterRebalance++;
        }

        assertEquals(1, checkpointsBeforeNodeStarted); // checkpoint on start
        assertEquals(0, checkpointsBeforeRebalance); // no checkpoints before rebalance
        assertEquals(0, checkpointsAfterRebalance); // no checkpoint because no WAL re-activation
    }
}
