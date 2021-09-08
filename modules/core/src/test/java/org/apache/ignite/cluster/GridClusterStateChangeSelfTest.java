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

package org.apache.ignite.cluster;

import java.util.Collection;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks {@link ClusterState} change.
 */
public class GridClusterStateChangeSelfTest extends GridCommonAbstractTest {
    /** Names of nodes. */
    private static final Collection<String> NODES_NAMES = U.sealList("server1", "server2", "client1", "client2", "daemon1", "daemon2");

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClientMode(igniteInstanceName.startsWith("client"))
            .setDaemon(igniteInstanceName.startsWith("daemon"))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        for (String name : NODES_NAMES)
            startGrid(name);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** */
    @Test
    public void testInactiveActive() {
        testStateChanged(ClusterState.INACTIVE, ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testInactiveReadOnly() {
        testStateChanged(ClusterState.INACTIVE, ClusterState.ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testActiveReadOnly() {
        testStateChanged(ClusterState.ACTIVE, ClusterState.ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testSetSameStateActive() {
        testSetSameState(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void testSetSameStateInactive() {
        testSetSameState(ClusterState.INACTIVE);
    }

    /** */
    @Test
    public void testSetSameStateReadOnly() {
        testSetSameState(ClusterState.ACTIVE_READ_ONLY);
    }

    /** */
    private void testSetSameState(ClusterState state) {
        for (String name : NODES_NAMES) {
            IgniteEx initiator = grid(name);

            if (initiator.cluster().state() != state)
                initiator.cluster().state(state);

            NODES_NAMES.forEach(n -> assertEquals(n, state, grid(n).cluster().state()));

            // Set the same state.
            initiator.cluster().state(state);

            NODES_NAMES.forEach(n -> assertEquals(n, state, grid(n).cluster().state()));
        }
    }

    /**
     * Checks that cluster correctly change state from {@code from} state to {@code to} state.
     *
     * @param from Initial state.
     * @param to Target state.
     */
    private void testStateChanged(ClusterState from, ClusterState to) {
        assertTrue(from.toString(), from != to);

        for (String name : NODES_NAMES) {
            IgniteEx initiator = grid(name);

            // Needs for checking in both directions.
            initiator.cluster().state(to);

            initiator.cluster().state(from);

            NODES_NAMES.forEach(n -> assertEquals(n, from, grid(n).cluster().state()));

            initiator.cluster().state(to);

            NODES_NAMES.forEach(n -> assertEquals(n, to, grid(n).cluster().state()));
        }
    }
}
