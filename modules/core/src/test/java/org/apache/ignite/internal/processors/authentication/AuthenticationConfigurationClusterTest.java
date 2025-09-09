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

package org.apache.ignite.internal.processors.authentication;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor.SECURITY_DISABLED_ERROR_MSG;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;

/**
 * Test for disabled {@link IgniteAuthenticationProcessor}.
 */
public class AuthenticationConfigurationClusterTest extends GridCommonAbstractTest {
    /**
     * @param idx Node index.
     * @param authEnabled Authentication enabled.
     * @param client Client node flag.
     * @return Ignite configuration.
     * @throws Exception On error.
     */
    private IgniteConfiguration configuration(int idx, boolean authEnabled, boolean client) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setClientMode(client);

        cfg.setAuthenticationEnabled(authEnabled);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        GridTestUtils.setFieldValue(User.class, "bCryptGensaltLog2Rounds", 4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        GridTestUtils.setFieldValue(User.class, "bCryptGensaltLog2Rounds", 10);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        recreateDefaultDb();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerNodeJoinDisabled() throws Exception {
        checkNodeJoinFailed(false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeJoinDisabled() throws Exception {
        checkNodeJoinFailed(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerNodeJoinEnabled() throws Exception {
        checkNodeJoinFailed(false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeJoinEnabled() throws Exception {
        checkNodeJoinFailed(true, true);
    }

    /**
     * Checks that a new node cannot join a cluster with a different authentication enable state.
     *
     * @param client Is joining node client.
     * @param authEnabled Whether authentication is enabled on server node, which accepts join of new node.
     * @throws Exception If failed.
     */
    private void checkNodeJoinFailed(boolean client, boolean authEnabled) throws Exception {
        startGrid(configuration(0, authEnabled, false));

        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(1, !authEnabled, client));

                    return null;
                }
            },
            IgniteSpiException.class,
            "Local node's grid security processor class is not equal to remote node's grid security processor class");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisabledAuthentication() throws Exception {
        startGrid(configuration(0, false, false));

        grid(0).cluster().state(ClusterState.ACTIVE);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().security().createUser("test", "test".toCharArray());

                    return null;
                }
            }, IgniteException.class, SECURITY_DISABLED_ERROR_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().security().dropUser("test");

                    return null;
                }
            }, IgniteException.class, SECURITY_DISABLED_ERROR_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().security().alterUser("test", "test".toCharArray());

                    return null;
                }
            }, IgniteException.class, SECURITY_DISABLED_ERROR_MSG);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    authenticate(grid(0), "test", "test");

                    return null;
                }
            }, IgniteException.class, SECURITY_DISABLED_ERROR_MSG);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnableAuthenticationWithoutPersistence() throws Exception {
        GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(0, true, false).setDataStorageConfiguration(null));

                    return null;
                }
            },
            IgniteCheckedException.class,
            "Authentication can be enabled only for cluster with enabled persistence");
    }

    /** Tests that authentication and security plugin can't be configured at the same time. */
    @Test
    public void testBothAuthenticationAndSecurityPluginConfiguration() {
        GridTestUtils.assertThrowsAnyCause(log, () -> {
            startGrid(configuration(0, true, false)
                .setPluginProviders(new TestSecurityPluginProvider("login", "", ALL_PERMISSIONS, false)));

            return null;
        },
            IgniteCheckedException.class,
            "Invalid security configuration: both authentication is enabled and external security plugin is provided.");
    }

    /**
     * Tests that client node configured with authentication but without persistence could start and join the cluster.
     */
    @Test
    public void testClientNodeWithoutPersistence() throws Exception {
        startGrid(configuration(0, true, false))
            .cluster().state(ACTIVE);

        IgniteConfiguration clientCfg = configuration(1, true, true);

        clientCfg.getDataStorageConfiguration()
            .getDefaultDataRegionConfiguration()
            .setPersistenceEnabled(false);

        startGrid(clientCfg);

        assertEquals("Unexpected cluster size", 2, grid(1).cluster().nodes().size());
    }
}
