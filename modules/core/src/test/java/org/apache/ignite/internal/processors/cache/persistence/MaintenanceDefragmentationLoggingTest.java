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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.CacheFileTree;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;

/**
 * Tests for maintenance logging of defragmentation task.
 * Verifies the full lifecycle logging.
 */
public class MaintenanceDefragmentationLoggingTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch markerReached;

    /** Listening test logger. */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setMaxSize(100 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setMetricsLogFrequency(1000);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        Arrays.stream(Lsnrs.values()).map(Lsnrs::lsnr).forEach(lsnr -> {
            lsnr.reset();
            listeningLog.registerListener(lsnr);
        });

        markerReached = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        markerReached = null;

        stopAllGrids();

        cleanPersistenceDir();

        listeningLog.clearListeners();

        super.afterTest();
    }

    /**
     * Tests the full lifecycle logging for defragmentation task:
     * ACTIVE -> COMPLETED.
     */
    @Test
    public void testLogNormalLifecycleDefragmentationTask() throws Exception {
        assertTrue(executeTest(false));
        assertTrue(Lsnrs.MAINT_MODE_ENABLED.lsnr().check(10000));
        assertTrue(Lsnrs.MAINT_MODE_BASIC_METRICS.lsnr().check(10000));
        assertTrue(Lsnrs.TASK_DFRG_ACTIVE.lsnr().check(10000));
        assertTrue(Lsnrs.TASK_DFRG_COMPLETED.lsnr().check(10000));
        assertTrue(Lsnrs.DFRG_COMPLETED.lsnr().check(10000));
    }

    /**
     * Tests logging when defragmentation is cancelled via stop action:
     * ACTIVE -> (stop action) -> CANCELLED.
     */
    @Test
    public void testLogRegisteredActiveCancelledUnregisteredDefragmentationTask() throws Exception {
        assertTrue(executeTest(false));
        assertTrue(Lsnrs.MAINT_MODE_ENABLED.lsnr().check(10000));
        assertTrue(Lsnrs.MAINT_MODE_BASIC_METRICS.lsnr().check(10000));
        assertTrue(Lsnrs.TASK_DFRG_ACTIVE.lsnr().check(10000));

        grid(0).context().maintenanceRegistry().actionsForMaintenanceTask(
                CachePartitionDefragmentationManager.DEFRAGMENTATION_MNTC_TASK_NAME).get(0).execute();

        assertTrue(Lsnrs.TASK_DFRG_CANCELLED.lsnr().check(10000));
    }

    /**
     * Tests logging when defragmentation fails:
     * ACTIVE -> FAILED.
     */
    @Test
    public void testLogRegisterActiveFailedDefragmentationTask() throws Exception {
        assertTrue(executeTest(true));
        assertTrue(Lsnrs.MAINT_MODE_ENABLED.lsnr().check(10000));
        assertTrue(Lsnrs.MAINT_MODE_BASIC_METRICS.lsnr().check(10000));
        assertTrue(Lsnrs.TASK_DFRG_ACTIVE.lsnr().check(10000));

        markerReached.countDown();

        assertTrue(Lsnrs.TASK_DFRG_FAILED.lsnr().check(10000));
    }

    /** */
    private void forceCheckpoint(IgniteEx ignite) throws Exception {
        ignite.context().cache().context().database()
            .forceCheckpoint("testDefrag")
            .futureFor(CheckpointState.FINISHED)
            .get();
    }

    /** */
    private boolean executeTest(boolean isFailed) throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().state(ClusterState.ACTIVE);

        ignite.createCache(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME).setBackups(1));

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 2048; i++)
            cache.put(i, i);

        forceCheckpoint(ignite);

        MaintenanceTask defragTask = toStore(Collections.singletonList(DEFAULT_CACHE_NAME));

        ignite.context().maintenanceRegistry().registerMaintenanceTask(defragTask);

        CacheFileTree cft = ignite.context().pdsFolderResolver().fileTree()
                .cacheTree(ignite.cachex(DEFAULT_CACHE_NAME).configuration());

        stopGrid(0);

        if (isFailed) {
            markerReached = new CountDownLatch(1);

            UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
                DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

                FileIOFactory delegate = dsCfg.getFileIOFactory();
                File marker = cft.defragmentationCompletionMarkerFile();

                dsCfg.setFileIOFactory((file, modes) -> {
                    if (file.equals(marker)) {
                        throw new IOException("Failed to create defragmentation completion marker.");
                    }

                    return delegate.create(file, modes);
                });

                return cfg;
            };

            ignite = startGrid(0, cfgOp);
        }
        else
            ignite = startGrid(0);

        return ignite.context().maintenanceRegistry().isMaintenanceMode();
    }

    /** */
    enum Lsnrs {
        /** */
        DFRG_COMPLETED(LogListener.matches("Defragmentation process completed.").build()),

        /** */
        MAINT_MODE_BASIC_METRICS(LogListener.matches("!!! ATTENTION! NODE IS IN MAINTENANCE MODE !!!").build()),

        /** */
        MAINT_MODE_ENABLED(LogListener.matches("ATTENTION! MAINTENANCE MODE ENABLED!").build()),

        /** */
        TASK_DFRG_ACTIVE(LogListener.matches(
            "^-- REGISTERED name=defragmentationMaintenanceTask, description=Caches defragmentation, params=[default], " +
                    "executedActions=[execute=ACTIVE]").build()),

        /** */
        TASK_DFRG_CANCELLED(LogListener.matches(
            "^-- UNREGISTERED name=defragmentationMaintenanceTask, description=Caches defragmentation, params=[default], " +
                    "executedActions=[execute=ACTIVE->CANCELLED, stop=ACTIVE->COMPLETED]").build()),

        /** */
        TASK_DFRG_COMPLETED(LogListener.matches(
            "^-- UNREGISTERED name=defragmentationMaintenanceTask, description=Caches defragmentation, params=[default], " +
                    "executedActions=[execute=ACTIVE->COMPLETED]").build()),

        /** */
        TASK_DFRG_FAILED(LogListener.matches(
            "^-- REGISTERED name=defragmentationMaintenanceTask, description=Caches defragmentation, params=[default], " +
                    "executedActions=[execute=ACTIVE->FAILED]").build());

        /** */
        private final LogListener lsnr;

        /** */
        Lsnrs(LogListener lsnr) {
            this.lsnr = lsnr;
        }

        /** */
        private LogListener lsnr() {
            return lsnr;
        }
    }
}
