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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE_ACTIVE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_CANCEL;
import static org.apache.ignite.plugin.security.SecurityPermission.TASK_EXECUTE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests permissions of cluster ctate change.
 */
public class ClusterStateSecurityTest extends AbstractSecurityTest {
    /** */
    private boolean isClient;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testActivationAllowed() throws Exception {
        testChangeClusterState(configuration(0, ADMIN_CLUSTER_STATE_ACTIVE), ClusterState.ACTIVE, true);
    }

    /** */
    @Test
    public void testActivationReadOnlyAllowed() throws Exception {
        testChangeClusterState(configuration(0, ADMIN_CLUSTER_STATE_ACTIVE), ClusterState.ACTIVE_READ_ONLY, true);
    }

    /** */
    @Test
    public void testActivationNotAllowed() throws Exception {
        testChangeClusterState(null, ClusterState.ACTIVE, false);
    }

    /** */
    @Test
    public void testActivationReadOnlyNotAllowed() throws Exception {
        testChangeClusterState(null, ClusterState.ACTIVE_READ_ONLY, false);
    }

    /** */
    private void testChangeClusterState(IgniteConfiguration cfg, ClusterState state, boolean allowed) throws Exception {
        if (cfg == null)
            cfg = configuration(0);

        Ignite ig = startGrid(cfg);

        ClusterState stateBefore = ig.cluster().state();

        if (allowed) {
            ig.cluster().state(state);

            assertNotEquals(stateBefore, ig.cluster().state());
            assertEquals(state, ig.cluster().state());
        }
        else {
            assertThrows(
                null,
                () -> {
                    ig.cluster().state(state);

                    return null;
                },
                org.apache.ignite.plugin.security.SecurityException.class,
                "Authorization failed [perm=ADMIN_CLUSTER_STATE_ACTIVE"
            );

            assertEquals(stateBefore, ig.cluster().state());
            assertNotEquals(state, ig.cluster().state());
        }
    }

    /** @return Ignite node configuration. */
    private IgniteConfiguration configuration(int idx, SecurityPermission... perms) throws Exception {
        String name = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(
            name,
            new TestSecurityPluginProvider(
                name,
                "",
                create()
                    .defaultAllowAll(false)
                    .appendSystemPermissions(JOIN_AS_SERVER)
                    .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE)
                    .appendTaskPermissions(
                        "org.apache.ignite.internal.processors.affinity.GridAffinityUtils$AffinityJob",
                        TASK_EXECUTE, TASK_CANCEL)
                    .appendSystemPermissions(perms)
                    .build(),
                null,
                false
            )
        );

        cfg.setClientMode(isClient);
        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        return cfg;
    }
}
