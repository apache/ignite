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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.alterUserPassword;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.createUser;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.dropUser;
import static org.apache.ignite.internal.processors.security.NoOpIgniteSecurityProcessor.SECURITY_DISABLED_ERROR_MSG;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

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

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);
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
     * @param  authEnabled Whether authentication is enabled on joining node.
     * @throws Exception If failed.
     */
    private void checkNodeJoinFailed(boolean client, boolean authEnabled) throws Exception {
        startGrid(configuration(0, authEnabled, false));

        GridTestUtils.assertThrowsAnyCause(log, () -> {
                startGrid(configuration(1, !authEnabled, client));

                return null;
            },
            IgniteSpiException.class,
            authEnabled ?
                "Failed to add node to topology because user authentication is enabled on cluster and the node doesn't support user authentication" :
                "Local node's grid security processor class is not equal to remote node's grid security processor class");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisabledAuthentication() throws Exception {
        startGrid(configuration(0, false, false));

        grid(0).cluster().active(true);

        checkSecurityOperationFailed(() -> createUser(grid(0), null, "test", "test"));
        checkSecurityOperationFailed(() -> dropUser(grid(0), null, "test"));
        checkSecurityOperationFailed(() -> alterUserPassword(grid(0), null, "test", "test"));
    }

    /** Checks that specified security operation fails. */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkSecurityOperationFailed(IgniteThrowableRunner op) {
        GridTestUtils.assertThrows(log, () -> {
            op.run();

            return null;
        }, IgniteCheckedException.class, SECURITY_DISABLED_ERROR_MSG);
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
                    .setPluginProviders(new TestSecurityPluginProvider("login", "", ALLOW_ALL, false)));

                return null;
            },
            IgniteCheckedException.class,
            "Invalid security configuration: both authentication is enabled and security plugin is provided.");
    }
}
