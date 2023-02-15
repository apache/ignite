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

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;

import static java.lang.Boolean.*;
import static org.apache.ignite.cluster.ClusterState.*;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE_ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE_INACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests permissions of cluster ctate change.
 */
public class ClusterStateSecurityTest extends AbstractSecurityTest {
    /** */
    private boolean isClient;

    /** */
    private boolean persistent = true;

    /** */
    private ClusterState startState = INACTIVE;

    /** */
    private SecurityPermission[] adminPerms;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);

        cfg.setPluginProviders(new TestSecurityPluginProvider(
            instanceName,
            "",
            create()
                .defaultAllowAll(false)
                .appendSystemPermissions(JOIN_AS_SERVER)
                .appendSystemPermissions(adminPerms)
                .build(),
            null,
            false
        ));

        cfg.setClientMode(isClient);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(persistent)));

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        cfg.setClusterStateOnStart(startState);

        return cfg;
    }

    /** */
    @Test
    public void testActivationDeactivationAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_STATE_ACTIVE, ADMIN_CLUSTER_STATE_INACTIVE);

        testChangeClusterState(
            F.asArray(ACTIVE, INACTIVE, ACTIVE_READ_ONLY, INACTIVE),
            F.asArray(TRUE, TRUE, TRUE, TRUE)
        );
    }

    /** */
    @Test
    public void testActivationAllowedDeactivationNotAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_STATE_ACTIVE);

        testChangeClusterState(F.asArray(ACTIVE, INACTIVE), F.asArray(TRUE, FALSE));
    }

    /** */
    @Test
    public void testActivationReadOnlyAllowedDeactivationNotAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_STATE_ACTIVE);

        testChangeClusterState(F.asArray(ACTIVE_READ_ONLY, INACTIVE), F.asArray(TRUE, FALSE));
    }

    /** */
    @Test
    public void testDeactivationAllowedActivationNotAllowed() throws Exception {
        adminPerms = F.asArray(ADMIN_CLUSTER_STATE_INACTIVE);

        startState = ACTIVE;

        testChangeClusterState(F.asArray(INACTIVE, ACTIVE, ACTIVE_READ_ONLY), F.asArray(TRUE, FALSE, FALSE));
    }

    /** */
    @Test
    public void testActiveIsAllowedAtStartByDefault() throws Exception {
        testAllowedAtStart(ACTIVE);
    }

    /** */
    @Test
    public void testInactiveReadOnlyIsAllowedAtStartByDefault() throws Exception {
        testAllowedAtStart(ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testInactiveIsAllowedAtStartByDefault() throws Exception {
        testAllowedAtStart(INACTIVE);
    }

    /** */
    private void testChangeClusterState(ClusterState[] states, Boolean[] allowed) throws Exception {
        assert states != null && allowed != null && states.length == allowed.length && states.length > 0;

        Ignite ig = startGrids(3);

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
                    "Authorization failed [perm=" + (stateTo == INACTIVE ? ADMIN_CLUSTER_STATE_INACTIVE :
                        ADMIN_CLUSTER_STATE_ACTIVE)
                );

                assertEquals(stateBefore, ig.cluster().state());
                assertNotEquals(stateTo, ig.cluster().state());
            }
        }
    }

    /** */
    private void testAllowedAtStart(ClusterState startState) throws Exception {
        this.startState = startState;

        adminPerms = F.asArray(startState == INACTIVE ? ADMIN_CLUSTER_STATE_ACTIVE : ADMIN_CLUSTER_STATE_INACTIVE);

        Ignite ig = startGrids(3);

        assertEquals(startState, ig.cluster().state());
    }
}
