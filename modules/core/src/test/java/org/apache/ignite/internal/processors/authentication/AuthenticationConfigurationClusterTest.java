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
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
        checkNodeJoinDisabled(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeJoinDisabled() throws Exception {
        checkNodeJoinDisabled(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServerNodeJoinEnabled() throws Exception {
        checkNodeJoinEnabled(false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientNodeJoinEnabled() throws Exception {
        checkNodeJoinEnabled(true);
    }

    /**
     * @param client Is joining node client.
     * @throws Exception If failed.
     */
    private void checkNodeJoinDisabled(boolean client) throws Exception {
        startGrid(configuration(0, true, false));

        startGrid(configuration(1, false, client));

        grid(0).cluster().active(true);

        AuthorizationContext actx = grid(1).context().authentication().authenticate("ignite", "ignite");

        assertNotNull(actx);

        assertEquals("ignite", actx.userName());
    }

    /**
     * @param client Is joining node client.
     * @throws Exception If failed.
     */
    private void checkNodeJoinEnabled(boolean client) throws Exception {
        startGrid(configuration(0, false, false));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(1, true, client));

                    return null;
                }
            },
            IgniteCheckedException.class,
            "User authentication is disabled on cluster");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDisabledAuthentication() throws Exception {
        startGrid(configuration(0, false, false));

        grid(0).cluster().active(true);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().authentication().addUser("test", "test");

                    return null;
                }
            }, IgniteException.class,
            "Can not perform the operation because the authentication is not enabled for the cluster");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().authentication().removeUser("test");

                    return null;
                }
            }, IgniteException.class,
            "Can not perform the operation because the authentication is not enabled for the cluster");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().authentication().updateUser("test", "test");

                    return null;
                }
            }, IgniteException.class,
            "Can not perform the operation because the authentication is not enabled for the cluster");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).context().authentication().authenticate("test", "test");

                    return null;
                }
            }, IgniteException.class,
            "Can not perform the operation because the authentication is not enabled for the cluster");
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
}
