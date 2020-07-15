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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests topology changes during collecting performance statistics.
 */
@RunWith(Parameterized.class)
public class TopologyChangesTest extends AbstractPerformanceStatisticsTest {
    /** Persistence anebled flag. */
    @Parameterized.Parameter
    public boolean persistence;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "persistence={0}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {{false}, {true}});
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(persistence)
                )
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        if (persistence)
            cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeJoin() throws Exception {
        startGrid(0);

        startCollectStatistics();

        startGrid(1);

        waitForStatisticsEnabled(true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterRestartWithPersistence() throws Exception {
        startGrids(2);

        if (persistence)
            grid(0).cluster().state(ClusterState.ACTIVE);

        startCollectStatistics();

        stopAllGrids(false);

        startGrids(2);

        if (persistence)
            grid(0).cluster().state(ClusterState.ACTIVE);

        waitForStatisticsEnabled(persistence);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClientReconnected() throws Exception {
        IgniteEx grid = startGrid(0);

        if (persistence)
            grid.cluster().state(ClusterState.ACTIVE);

        startCollectStatistics();

        startClientGrid(1);

        waitForStatisticsEnabled(true);

        stopGrid(0);

        grid = startGrid(0);

        if (persistence)
            grid.cluster().state(ClusterState.ACTIVE);

        waitForTopology(2);

        waitForStatisticsEnabled(persistence);
    }
}
