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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;

/** */
public class IgnitePdsDefragmentationTest extends GridCommonAbstractTest {
    /** */
    private volatile FailureContext failureContext;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler() {
            @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
                boolean res = super.handle(ignite, failureCtx);

                if(failureCtx.error() != null)
                    failureContext = failureCtx;

                return res;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setInitialSize(70L * 1024 * 1024)
                .setMaxSize(70L * 1024 * 1024)
                .setPersistenceEnabled(true)
        );

        dsCfg.setDefragmentationThreadPoolSize(8);

        dsCfg.setMaxWalArchiveSize(3L * 1024L * 1024L * 1024L);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<?, ?> cache1Cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(ATOMIC);

        cfg.setCacheConfiguration(cache1Cfg);

        return cfg;
    }


    /** */
    @Test
    public void testDefragmentationOom() throws Exception {
        // Increase if there is no OOM.
        int loadCnt = 800_000;

        int entrySize = 3900;

        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        byte[] val = new byte[entrySize];

        try (IgniteDataStreamer<Object, Object> ds = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < loadCnt; i++) {
                new Random().nextBytes(val);

                ds.addData(i, val);
            }
        }

        forceCheckpoint(ig);

        createMaintenanceRecord();

        stopGrid(0);

        startGrid(0);

        GridTestUtils.waitForCondition(() -> failureContext != null, getTestTimeout());

        assertTrue(failureContext.error() instanceof IgniteOutOfMemoryException);
    }

    /** */
    protected void createMaintenanceRecord(String... cacheNames) throws IgniteCheckedException {
        IgniteEx grid = grid(0);

        MaintenanceRegistry mntcReg = grid.context().maintenanceRegistry();

        final List<String> caches = new ArrayList<>();

        caches.add(DEFAULT_CACHE_NAME);

        if (cacheNames != null && cacheNames.length != 0)
            caches.addAll(Arrays.asList(cacheNames));

        mntcReg.registerMaintenanceTask(toStore(caches));
    }
}
