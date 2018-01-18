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
package org.apache.ignite.internal.processors.database.baseline;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgniteBaselineLockPartitionOnAffinityRunTxCacheTest extends IgniteCacheLockPartitionOnAffinityRunTxCacheOpTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setInitialSize(200 * 1024 * 1024)
                        .setMaxSize(200 * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        GridTestUtils.deleteDbFiles();

        int gridCnt = gridCount();

        startGrids(gridCnt + 1);

        grid(0).active(true);

        stopGrid(gridCnt);

        startGrid(gridCnt + 1);

        fillCaches();

        createCaches();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        destroyCaches();

        stopAllGrids();

        GridTestUtils.deleteDbFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        GridTestUtils.deleteDbFiles();
    }
}
