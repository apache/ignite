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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxLog;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointState;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.LightweightCheckpointManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_DATA_REGION_NAME;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class LightweightCheckpointTest extends GridCommonAbstractTest {
    /** Data region which should not be checkpointed. */
    public static final String NOT_CHECKPOINTED_REGION = "NotCheckpointedRegion";

    /** Cache which should not be checkpointed. */
    public static final String NOT_CHECKPOINTED_CACHE = "notCheckpointedCache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        DataStorageConfiguration storageCfg = new DataStorageConfiguration();

        storageCfg.setWalMode(WALMode.NONE);
        storageCfg.setCheckpointFrequency(100_000);
        storageCfg.setDataRegionConfigurations(new DataRegionConfiguration()
            .setName(NOT_CHECKPOINTED_REGION)
            .setPersistenceEnabled(true)
            .setMaxSize(300L * 1024 * 1024)

        );
        storageCfg.getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setMaxSize(300L * 1024 * 1024);

        cfg.setDataStorageConfiguration(storageCfg)

            .setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setAffinity(new RendezvousAffinityFunction(false, 16))
                    .setDataRegionName(DFLT_DATA_REG_DEFAULT_NAME),
                new CacheConfiguration<>(NOT_CHECKPOINTED_CACHE)
                    .setAffinity(new RendezvousAffinityFunction(false, 16))
                    .setDataRegionName(NOT_CHECKPOINTED_REGION)
            );

        return cfg;
    }

    /**
     * 1. Start the one node with disabled WAL and with two caches.
     * 2. Disable default checkpoint.
     * 3. Create light checkpoint for one cache and configure checkpoint listener for it.
     * 4. Fill the both caches.
     * 5. Trigger the light checkpoint and wait for the finish.
     * 6. Stop the node and start it again.
     * 7. Expected: Cache which was checkpointed would have the all data meanwhile second cache would be empty.
     *
     * @throws Exception if fail.
     */
    @Test
    public void testLightCheckpointAbleToStoreOnlyGivenDataRegion() throws Exception {
        //given: One started node with default cache and cache which won't be checkpointed.
        IgniteEx ignite0 = startGrid(0);
        ignite0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Object> checkpointedCache = ignite0.cache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Object> notCheckpointedCache = ignite0.cache(NOT_CHECKPOINTED_CACHE);

        GridKernalContext context = ignite0.context();
        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)(context.cache().context().database());

        waitForCondition(() -> !db.getCheckpointer().currentProgress().inProgress(), 10_000);

        //and: disable the default checkpoint.
        db.enableCheckpoints(false);

        DataRegion regionForCheckpoint = db.dataRegion(DFLT_DATA_REG_DEFAULT_NAME);

        //and: Create light checkpoint with only one region.
        LightweightCheckpointManager lightweightCheckpointManager = new LightweightCheckpointManager(
            context::log,
            context.igniteInstanceName(),
            "light-test-checkpoint",
            context.workersRegistry(),
            context.config().getDataStorageConfiguration(),
            () -> Arrays.asList(regionForCheckpoint),
            grpId -> getPageMemoryForCacheGroup(grpId, db, context),
            PageMemoryImpl.ThrottlingPolicy.CHECKPOINT_BUFFER_ONLY,
            context.cache().context().snapshot(),
            db.dataStorageMetricsImpl(),
            context.longJvmPauseDetector(),
            context.failure(),
            context.cache()
        );

        //and: Add checkpoint listener for DEFAULT_CACHE in order of storing the meta pages.
        lightweightCheckpointManager.addCheckpointListener(
            (CheckpointListener)context.cache().cacheGroup(groupIdForCache(ignite0, DEFAULT_CACHE_NAME)).offheap(),
            regionForCheckpoint
        );

        lightweightCheckpointManager.start();

        //when: Fill the caches
        for (int j = 0; j < 1024; j++) {
            checkpointedCache.put(j, j);
            notCheckpointedCache.put(j, j);
        }

        //and: Trigger and wait for the checkpoint.
        lightweightCheckpointManager.forceCheckpoint("test", null)
            .futureFor(CheckpointState.FINISHED)
            .get();

        //and: Stop and start node.
        stopAllGrids();

        ignite0 = startGrid(0);
        ignite0.cluster().state(ClusterState.ACTIVE);

        checkpointedCache = ignite0.cache(DEFAULT_CACHE_NAME);
        notCheckpointedCache = ignite0.cache(NOT_CHECKPOINTED_CACHE);

        //then: Checkpointed cache should have all data meanwhile uncheckpointed cache should be empty.
        for (int j = 1; j < 1024; j++) {
            assertEquals(j, checkpointedCache.get(j));
            assertNull(notCheckpointedCache.get(j));
        }

        GridCacheDatabaseSharedManager db2 = (GridCacheDatabaseSharedManager)
            (ignite0.context().cache().context().database());

        waitForCondition(() -> !db2.getCheckpointer().currentProgress().inProgress(), 10_000);

        String nodeFolderName = ignite0.context().pdsFolderResolver().resolveFolders().folderName();
        File cpMarkersDir = Paths.get(U.defaultWorkDirectory(), "db", nodeFolderName, "cp").toFile();

        //then: Expected only two pairs checkpoint markers - both from the start of node.
        assertEquals(4, cpMarkersDir.listFiles().length);
    }

    /**
     * @return Page memory which corresponds to grpId.
     */
    private PageMemoryEx getPageMemoryForCacheGroup(
        int grpId,
        GridCacheDatabaseSharedManager db,
        GridKernalContext context
    ) throws IgniteCheckedException {
        if (grpId == MetaStorage.METASTORAGE_CACHE_ID)
            return (PageMemoryEx)db.dataRegion(METASTORE_DATA_REGION_NAME).pageMemory();

        if (grpId == TxLog.TX_LOG_CACHE_ID)
            return (PageMemoryEx)db.dataRegion(TxLog.TX_LOG_CACHE_NAME).pageMemory();

        CacheGroupDescriptor desc = context.cache().cacheGroupDescriptors().get(grpId);

        if (desc == null)
            return null;

        String memPlcName = desc.config().getDataRegionName();

        return (PageMemoryEx)context.cache().context().database().dataRegion(memPlcName).pageMemory();
    }
}
