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

package org.apache.ignite.internal.processors.security.cluster;

import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;

/**
 * Tests joining baseline node without permissions. It should join successfully, but
 * cluster must be left in INACTIVE state. {@link org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor#onLocalJoin}
 * must not complete exceptionally.
 */
public class ActivationOnJoinWithoutPermissionsWithPersistenceTest extends AbstractSecurityTest {
    /** */
    private static final int NUM_NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        SecurityPermission[] srvPerms = F.asArray(ADMIN_CLUSTER_STATE);

        return getConfiguration(instanceName, srvPerms);
    }

    /**
     * @return Ignite configuration with given server and client permissions.
     */
    private IgniteConfiguration getConfiguration(
        String instanceName,
        SecurityPermission[] srvPerms
    ) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setConsistentId(instanceName);

        TestSecurityPluginProvider secPlugin = new TestSecurityPluginProvider(
            instanceName,
            "",
            create().defaultAllowAll(false).appendSystemPermissions(F.concat(srvPerms, JOIN_AS_SERVER)).build(),
            false,
            EMPTY_SECURITY_DATA
        );

        cfg.setPluginProviders(secPlugin);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Test
    public void testNodeJoinWithoutPermissions() throws Exception {
        // Start grids and activate them.
        startGrids(NUM_NODES);

        grid(0).cluster().state(ACTIVE);
        assertEquals(ACTIVE, grid(0).cluster().state());

        // Stop all of them and restart just the firsts.
        G.stopAll(true);

        startGrids(NUM_NODES - 1);

        // Start the last one with empty permissions.
        startGrid(getConfiguration(getTestIgniteInstanceName(NUM_NODES - 1), EMPTY_PERMS));

        // Check for state and topology.
        waitForTopology(NUM_NODES);

        assertEquals(INACTIVE, grid(0).cluster().state());
        assertEquals(NUM_NODES, grid(0).cluster().forServers().nodes().size());
    }
}
