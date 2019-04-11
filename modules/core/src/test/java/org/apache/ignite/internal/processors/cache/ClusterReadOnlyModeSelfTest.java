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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertCachesReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.assertDataStreamerReadOnlyMode;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheConfigurations;
import static org.apache.ignite.internal.processors.cache.ClusterReadOnlyModeTestUtils.cacheNames;

/**
 *
 */
public class ClusterReadOnlyModeSelfTest extends GridCommonAbstractTest {
    /** Server nodes count. */
    private static final int SERVER_NODES_COUNT = 2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setClientMode("client".equals(igniteInstanceName))
            .setCacheConfiguration(cacheConfigurations())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testChangeReadOnlyModeOnInactiveClusterFails() throws Exception {
        startGrid(0);

        GridTestUtils.assertThrows(
            log,
            () -> {
                grid(0).cluster().readOnly(true);

                return null;
            },
            IgniteException.class,
            "Cluster not active!"
        );
    }

    /** */
    @Test
    public void testReadOnlyActivationOnActiveClusterFails() throws Exception {
        startGrid(0).cluster().active(true);

        GridTestUtils.assertThrows(
            log,
            () -> {
                grid(0).cluster().activeReadOnly();

                return null;
            },
            IgniteException.class,
            "Cluster already active!"
        );
    }

    /** */
    @Test
    public void testClusterForgetReadOnlyStateAfterDeactivation() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().activeReadOnly();

        checkClusterInReadOnlyMode(true, grid);

        grid.cluster().active(false);

        assertFalse("Grid should forget previous read-only mode after reboot!", grid.cluster().readOnly());

        grid.cluster().active(true);

        checkClusterInReadOnlyMode(false, grid);
    }

    /** */
    @Test
    public void testClusterForgetReadOnlyStateOnRestart() throws Exception {
        IgniteEx grid = startGrid(0);

        grid(0).cluster().activeReadOnly();

        checkClusterInReadOnlyMode(true, grid);

        stopGrid(0);
        grid = startGrid(0);

        assertTrue("Grid should be active!", grid.cluster().active());
        assertFalse("Grid should forget previous read-only mode after reboot!", grid.cluster().readOnly());

        checkClusterInReadOnlyMode(false, grid);
    }

    /** */
    @Test
    public void testReadOnlyFromClient() throws Exception {
        startGrids(SERVER_NODES_COUNT);
        startGrid("client");

        grid(0).cluster().active(true);

        awaitPartitionMapExchange();

        IgniteEx client = grid("client");

        assertTrue("Should be client!", client.configuration().isClientMode());

        checkClusterInReadOnlyMode(false, client);

        client.cluster().readOnly(true);

        checkClusterInReadOnlyMode(true, client);

        client.cluster().readOnly(false);

        checkClusterInReadOnlyMode(false, client);
    }

    /** */
    @Test
    public void testActivateReadOnly() throws Exception {
        startGrids(SERVER_NODES_COUNT).cluster().activeReadOnly();

        checkClusterInReadOnlyMode(true, grid(0));
    }

    /** */
    private void checkClusterInReadOnlyMode(boolean readOnly, IgniteEx node) {
        assertEquals("Unexpected read-only mode", readOnly, node.cluster().readOnly());

        assertCachesReadOnlyMode(readOnly, cacheNames());

        assertDataStreamerReadOnlyMode(readOnly, cacheNames());
    }
}
