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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.apache.ignite.mxbean.DefragmentationMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;

public class DefragmentationMXBeanTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch blockCdl;

    /** */
    private static CountDownLatch waitCdl;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        final DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setWalSegmentSize(512 * 1024).setWalSegments(3);
        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(true)
        );

        return cfg.setDataStorageConfiguration(dsCfg);
    }

    @Test
    public void testDefragmentationSchedule() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        DefragmentationMXBean mxBean = defragmentationMXBean(ignite.name());

        assertTrue(mxBean.schedule(""));

        MaintenanceTask mntcTask = DefragmentationParameters.toStore(Collections.emptyList());

        assertNotNull(grid(0).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));
        assertNull(grid(1).context().maintenanceRegistry().registerMaintenanceTask(mntcTask));

        stopGrid(0);
        startGrid(0);

        // node is already in defragmentation mode, hence scheduling is not possible
        assertFalse(mxBean.schedule(""));
    }

    @Test
    public void testDefragmentationCancel() throws Exception {
        Ignite ignite = startGrids(2);

        ignite.cluster().state(ACTIVE);

        final DefragmentationMXBean mxBean = defragmentationMXBean(ignite.name());

        assertTrue(mxBean.cancel());

        // subsequent cancel call should be successful
        assertTrue(mxBean.cancel());
    }

    @Test
    public void testDefragmentationCancelInProgress() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        DefragmentationMXBean mxBean = defragmentationMXBean(ig.name());

        mxBean.schedule("");

        stopGrid(0);

        blockCdl = new CountDownLatch(128);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            // Slow down defragmentation process.
                            // This'll be enough for the test since we have, like, 900 partitions left.
                            Thread.sleep(100);
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        mxBean = defragmentationMXBean(ig.name());

        assertTrue(mxBean.cancel());

        fut.get();

        assertTrue(mxBean.cancel());
    }

    @Test
    public void testDefragmentationStatus() throws Exception {
        IgniteEx ig = startGrid(0);

        ig.cluster().state(ClusterState.ACTIVE);

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "1");

        IgniteCache<Object, Object> cache = ig.getOrCreateCache(DEFAULT_CACHE_NAME + "2");

        ig.getOrCreateCache(DEFAULT_CACHE_NAME + "3");

        for (int i = 0; i < 1024; i++)
            cache.put(i, i);

        forceCheckpoint(ig);

        DefragmentationMXBean mxBean = defragmentationMXBean(ig.name());

        mxBean.schedule("");

        stopGrid(0);

        blockCdl = new CountDownLatch(128);
        waitCdl = new CountDownLatch(1);

        UnaryOperator<IgniteConfiguration> cfgOp = cfg -> {
            DataStorageConfiguration dsCfg = cfg.getDataStorageConfiguration();

            FileIOFactory delegate = dsCfg.getFileIOFactory();

            dsCfg.setFileIOFactory((file, modes) -> {
                if (file.getName().contains("dfrg")) {
                    if (blockCdl.getCount() == 0) {
                        try {
                            waitCdl.await();
                        }
                        catch (InterruptedException ignore) {
                            // No-op.
                        }
                    }
                    else
                        blockCdl.countDown();
                }

                return delegate.create(file, modes);
            });

            return cfg;
        };

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try {
                startGrid(0, cfgOp);
            }
            catch (Exception e) {
                // No-op.
                throw new RuntimeException(e);
            }
        });

        blockCdl.await();

        mxBean = defragmentationMXBean(ig.name());

        final IgniteKernal gridx = IgnitionEx.gridx(ig.name());
        final IgniteDefragmentation defragmentation = gridx.context().defragmentation();
        final IgniteDefragmentation.DefragmentationStatus status1 = defragmentation.status();

        assertEquals(status1.getStartTs(), mxBean.startTime());

        assertTrue(mxBean.inProgress());
        assertEquals(126, mxBean.processedPartitions());
        final int totalPartitions = status1.getTotalPartitions();
        assertEquals(totalPartitions, mxBean.totalPartitions());

        waitCdl.countDown();

        fut.get();

        ((GridCacheDatabaseSharedManager) grid(0).context().cache().context().database())
            .defragmentationManager()
            .completionFuture()
            .get();

        assertFalse(mxBean.inProgress());
        assertEquals(totalPartitions, mxBean.processedPartitions());
    }

    private DefragmentationMXBean defragmentationMXBean(String name) {
        return getMxBean(
            name,
            "Defragmentation",
            DefragmentationMXBeanImpl.class,
            DefragmentationMXBean.class
        );
    }

}
