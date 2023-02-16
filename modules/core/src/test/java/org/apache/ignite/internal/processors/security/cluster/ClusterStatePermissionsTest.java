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

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_ACTIVATE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_DEACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests permissions of cluster ctate change.
 */
@RunWith(Parameterized.class)
public class ClusterStatePermissionsTest extends AbstractSecurityTest {
    /** Persistence flag. */
    @Parameterized.Parameter(0)
    public boolean persistence;

    /** From-client flag. */
    @Parameterized.Parameter(1)
    public boolean client;

    /** From-thin-client flag. */
    @Parameterized.Parameter(3)
    public boolean thinClient;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "persistence={0}, fromClient={1}, thinClient={2}")
    public static Collection<?> parameters() {
        return Arrays.asList(new Object[][] {
            {false, false, false},
            {false, true, false},
            {false, true, true},
            {true, false, false},
            {true, true, false},
            {true, true, true},
        });
    }

    /** */
    private ClusterState startState = INACTIVE;

    /** */
    private SecurityPermission[] adminPerms = F.asArray(ADMIN_CLUSTER_ACTIVATE, ADMIN_CLUSTER_DEACTIVE);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        SecurityPermissionSetBuilder secBuilder = create().defaultAllowAll(false);

        if (cfg.isClientMode()) {
            secBuilder.appendSystemPermissions(adminPerms);

            secBuilder.appendTaskPermissions(
                "org.apache.ignite.internal.processors.cluster.ClientSetClusterStateComputeRequest",
                TASK_EXECUTE
            );
        }
        else {
            secBuilder.appendSystemPermissions(F.concat(adminPerms, JOIN_AS_SERVER, CACHE_CREATE));

            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistence)));

            cfg.setCacheConfiguration(defaultCacheConfiguration());

            cfg.setClusterStateOnStart(startState);
        }

        TestSecurityPluginProvider secPlugin = new TestSecurityPluginProvider(
            instanceName,
            "",
            secBuilder.build(),
            null,
            false
        );

        cfg.setPluginProviders(secPlugin);

        return cfg;
    }

    /** */
    @Test
    public void testActivationDeactivationAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_ACTIVATE, ADMIN_CLUSTER_DEACTIVE);

        testChangeClusterState(
            F.asArray(ACTIVE, INACTIVE, ACTIVE_READ_ONLY, INACTIVE),
            F.asArray(TRUE, TRUE, TRUE, TRUE)
        );
    }

    /** */
    @Test
    public void testActivationAllowedDeactivationNotAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_ACTIVATE);

        testChangeClusterState(F.asArray(ACTIVE, INACTIVE), F.asArray(TRUE, FALSE));
    }

    /** */
    @Test
    public void testActivationReadOnlyAllowedDeactivationNotAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_ACTIVATE);

        testChangeClusterState(F.asArray(ACTIVE_READ_ONLY, INACTIVE), F.asArray(TRUE, FALSE));
    }

    /** */
    @Test
    public void testDeactivationAllowedActivationNotAllowed() throws Exception {
        Ignite ig = startGrids(1);

        ig.cluster().state(ACTIVE);

        adminPerms = F.asArray(ADMIN_CLUSTER_DEACTIVE);

        testChangeClusterState(F.asArray(INACTIVE, ACTIVE, ACTIVE_READ_ONLY), F.asArray(TRUE, FALSE, FALSE));
    }

    /** */
    private void testChangeClusterState(ClusterState[] states, Boolean[] allowed) throws Exception {
        assert states != null && allowed != null && states.length == allowed.length && states.length > 0;

        assert !thinClient || client;

        Ignite ig0 = startGrid(G.allGrids().size());

        startGrid(G.allGrids().size());
        startGrid(G.allGrids().size());

        Ignite ig = client ? startClientGrid(G.allGrids().size()) : ig0;

        assert !client || ig.configuration().isClientMode();

        for (int i = 0; i < states.length; ++i) {
            assert allowed[i] != null;

            ClusterState stateBefore = ig.cluster().state();
            ClusterState stateTo = states[i];

            assert stateBefore != stateTo;

            if (allowed[i]) {
                ig.cluster().state(stateTo);

                assertNotEquals(stateBefore, ig.cluster().state());
                assertEquals(stateTo, ig.cluster().state());
            }
            else {
                assertThrows(
                    null,
                    () -> {
                        ig.cluster().state(stateTo);

                        return null;
                    },
                    org.apache.ignite.plugin.security.SecurityException.class,
                    "Authorization failed [perm=" + (stateTo == INACTIVE ? ADMIN_CLUSTER_DEACTIVE :
                        ADMIN_CLUSTER_ACTIVATE)
                );

                assertEquals(stateBefore, ig.cluster().state());
                assertNotEquals(stateTo, ig.cluster().state());
            }
        }
    }

    /** */
    private void testAllowedAtStart(ClusterState startState) throws Exception {
        this.startState = startState;

        adminPerms = F.asArray(startState == INACTIVE ? ADMIN_CLUSTER_ACTIVATE : ADMIN_CLUSTER_DEACTIVE);

        Ignite ig = startGrids(3);

        assertEquals(startState, ig.cluster().state());
    }
}
