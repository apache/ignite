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

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.AuthenticationConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link IgniteCompute#affinityCall(String, Object, IgniteCallable)} and
 * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)}.
 */
public class AuthenticationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes count. */
    private static final int NODES_COUNT = 3;

    /** Client node. */
    private static final int CLI_NODE = NODES_COUNT - 1;

    /** Random. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Authorization context for default user. */
    private AuthorizationContext actxDflt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == CLI_NODE)
            cfg.setClientMode(true);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setAuthenticationConfiguration(new AuthenticationConfiguration()
                .setEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(NODES_COUNT);

        actxDflt = grid(0).context().authentication().authenticate(User.DFAULT_USER_NAME, "ignite");

        assertNotNull(actxDflt);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            AuthorizationContext actx = grid(0).context().authentication().authenticate("ignite", "ignite");

            assertNotNull(actx);
            assertEquals("ignite", actx.userName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemoveDefault() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            final int nodeIdx = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().removeUser(actxDflt, "ignite");

                    return null;
                }
            }, IgniteAccessControlException.class, "Default user cannot be removed");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUserManagementPermission() throws Exception {
        grid(0).context().authentication().addUser(actxDflt, "test", "test");

        final AuthorizationContext actx = grid(0).context().authentication().authenticate("test", "test");

        for (int i = 0; i < NODES_COUNT; ++i) {
            final int nodeIdx = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().addUser(actx, "test1", "test1");

                    return null;
                }
            }, IgniteAccessControlException.class, "Add / remove user is not allowed for user");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().removeUser(actx, "test");

                    return null;
                }
            }, IgniteAccessControlException.class, "Add / remove user is not allowed for user");

            grid(nodeIdx).context().authentication().updateUser(actx, "test", "new_password");

            grid(nodeIdx).context().authentication().updateUser(actx, "test", "test");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testProceedUsersOnJoinNode() throws Exception {
        grid(0).context().authentication().addUser(actxDflt, "test0", "test");
        grid(0).context().authentication().addUser(actxDflt, "test1", "test");

        int nodeIdx = NODES_COUNT;

        startGrid(nodeIdx);

        AuthorizationContext actx0 = grid(nodeIdx).context().authentication().authenticate("test0", "test");
        AuthorizationContext actx1 = grid(nodeIdx).context().authentication().authenticate("test1", "test");

        assertNotNull(actx0);
        assertEquals("test0", actx0.userName());
        assertNotNull(actx1);
        assertEquals("test1", actx1.userName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAuthenticationInvalidUser() throws Exception {
        AuthorizationContext actxDflt = grid(CLI_NODE).context().authentication().authenticate("ignite", "ignite");

        grid(CLI_NODE).context().authentication().addUser(actxDflt, "test", "test");

        for (int i = 0; i < NODES_COUNT; ++i) {
            final int nodeIdx = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().authenticate("invalid_name", "test");

                    return null;
                }
            }, UserAuthenticationException.class, "The user name or password is incorrect");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().authenticate("test", "invalid_password");

                    return null;
                }
            }, UserAuthenticationException.class, "The user name or password is incorrect");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddRemoveUser() throws Exception {
        for (int i = 0; i< NODES_COUNT; ++i)
            for (int j = 0; j< NODES_COUNT; ++j)
                checkAddRemoveUser(grid(i), grid(j));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateUser() throws Exception {
        grid(0).context().authentication().addUser(actxDflt, "test", "test");

        AuthorizationContext actx = grid(0).context().authentication().authenticate("test", "test");

        for (int i = 0; i< NODES_COUNT; ++i)
            for (int j = 0; j< NODES_COUNT; ++j)
                checkUpdateUser(actx, grid(i), grid(j));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateRemoveDoesNotExistsUser() throws Exception {
        for (int i = 0; i< NODES_COUNT; ++i) {
            final int nodeIdx = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().updateUser(actxDflt, "invalid_name", "test");

                    return null;
                }
            }, UserAuthenticationException.class, "User doesn't exist");

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().removeUser(actxDflt, "invalid_name");

                    return null;
                }
            }, UserAuthenticationException.class, "User doesn't exist");
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddAlreadyExistsUser() throws Exception {
        grid(0).context().authentication().addUser(actxDflt, "test", "test");

        for (int i = 0; i< NODES_COUNT; ++i) {
            final int nodeIdx = i;

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(nodeIdx).context().authentication().addUser(actxDflt, "test", "new_passwd");

                    return null;
                }
            }, UserAuthenticationException.class, "User already exists");
        }
    }

    /**
     * @param createNode Node to execute create operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkAddRemoveUser(IgniteEx createNode, IgniteEx authNode) throws Exception {
        createNode.context().authentication().addUser(actxDflt, "test", "test");

        AuthorizationContext newActx = authNode.context().authentication().authenticate("test", "test");

        assertNotNull(newActx);
        assertEquals("test", newActx.userName());

        createNode.context().authentication().removeUser(actxDflt, "test");
    }

    /**
     * @param actx Authorization context.
     * @param updNode Node to execute update operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkUpdateUser(AuthorizationContext actx, IgniteEx updNode, IgniteEx authNode) throws Exception {
        String newPasswd = randomString(16);

        updNode.context().authentication().updateUser(actx, "test", newPasswd);

        AuthorizationContext actxNew = authNode.context().authentication().authenticate("test", newPasswd);

        assertNotNull(actxNew);
        assertEquals("test", actxNew.userName());
    }

    /**
     * @param len String length.
     * @return Random string (Base64 on random bytes).
     */
    private static String randomString(int len) {
        byte [] rndBytes = new byte[len / 2];

        RND.nextBytes(rndBytes);

        return Base64.getEncoder().encodeToString(rndBytes);
    }
}
