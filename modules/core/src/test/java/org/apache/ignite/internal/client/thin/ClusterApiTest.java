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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.client.ClientCluster;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 * Checks cluster state/WAL state operations for thin client.
 */
public class ClusterApiTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)))
            .setClusterStateOnStart(ClusterState.INACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /**
     * Test change cluster state operation by thin client.
     */
    @Test
    public void testClusterState() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCluster clientCluster = client.cluster();
            IgniteCluster igniteCluster = grid(0).cluster();

            changeAndCheckState(clientCluster, igniteCluster, ClusterState.INACTIVE);
            changeAndCheckState(clientCluster, igniteCluster, ClusterState.ACTIVE_READ_ONLY);
            changeAndCheckState(clientCluster, igniteCluster, ClusterState.ACTIVE);
            changeAndCheckState(clientCluster, igniteCluster, ClusterState.INACTIVE);
        }
    }

    /**
     * Test change WAL state for cache operation by thin client.
     */
    @Test
    public void testWalState() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCluster clientCluster = client.cluster();
            IgniteCluster igniteCluster = grid(0).cluster();

            igniteCluster.state(ClusterState.ACTIVE);

            grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

            igniteCluster.disableWal(DEFAULT_CACHE_NAME);

            // Check enable WAL operation.
            assertTrue(clientCluster.enableWal(DEFAULT_CACHE_NAME));
            assertTrue(clientCluster.isWalEnabled(DEFAULT_CACHE_NAME));
            assertTrue(igniteCluster.isWalEnabled(DEFAULT_CACHE_NAME));

            // Check enable WAL operation on already enabled WAL.
            assertFalse(clientCluster.enableWal(DEFAULT_CACHE_NAME));
            assertTrue(clientCluster.isWalEnabled(DEFAULT_CACHE_NAME));
            assertTrue(igniteCluster.isWalEnabled(DEFAULT_CACHE_NAME));

            // Check disable WAL operation.
            assertTrue(clientCluster.disableWal(DEFAULT_CACHE_NAME));
            assertFalse(clientCluster.isWalEnabled(DEFAULT_CACHE_NAME));
            assertFalse(igniteCluster.isWalEnabled(DEFAULT_CACHE_NAME));

            // Check disable WAL operation on already disabled WAL.
            assertFalse(clientCluster.disableWal(DEFAULT_CACHE_NAME));
            assertFalse(clientCluster.isWalEnabled(DEFAULT_CACHE_NAME));
            assertFalse(igniteCluster.isWalEnabled(DEFAULT_CACHE_NAME));
        }
    }

    /**
     * Changes state by thin client and check by thin and thick clients that state is changed.
     */
    private void changeAndCheckState(ClientCluster clientCluster, IgniteCluster igniteCluster, ClusterState state) {
        clientCluster.state(state);

        assertEquals(state, clientCluster.state());
        assertEquals(state, igniteCluster.state());
    }
}
