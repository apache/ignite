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

import java.util.Base64;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link IgniteAuthenticationProcessor}.
 */
public class AuthenticationProcessorSelfTest extends GridCommonAbstractTest {
    /** Nodes count. */
    protected static final int NODES_COUNT = 4;

    /** Iterations count. */
    private static final int ITERATIONS = 10;

    /** Client node. */
    protected static final int CLI_NODE = NODES_COUNT - 1;

    /** Random. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** Authorization context for default user. */
    protected AuthorizationContext actxDflt;

    /**
     * @param len String length.
     * @return Random string (Base64 on random bytes).
     */
    private static String randomString(int len) {
        byte[] rndBytes = new byte[len / 2];

        RND.nextBytes(rndBytes);

        return Base64.getEncoder().encodeToString(rndBytes);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

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
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        grid(0).cluster().active(true);

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
    @Test
    public void testDefaultUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            AuthorizationContext actx = grid(i).context().authentication().authenticate("ignite", "ignite");

            assertNotNull(actx);
            assertEquals("ignite", actx.userName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultUserUpdate() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            // Change from all nodes
            for (int nodeIdx = 0; nodeIdx < NODES_COUNT; ++nodeIdx) {
                grid(nodeIdx).context().authentication().updateUser("ignite", "ignite" + nodeIdx);

                // Check each change from all nodes
                for (int i = 0; i < NODES_COUNT; ++i) {
                    AuthorizationContext actx = grid(i).context().authentication().authenticate("ignite", "ignite" + nodeIdx);

                    assertNotNull(actx);
                    assertEquals("ignite", actx.userName());
                }
            }
        }
        finally {
            AuthorizationContext.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveDefault() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().removeUser("ignite");

                        return null;
                    }
                }, IgniteAccessControlException.class, "Default user cannot be removed");

                assertNotNull(grid(nodeIdx).context().authentication().authenticate("ignite", "ignite"));
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserManagementPermission() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            grid(0).context().authentication().addUser("test", "test");

            final AuthorizationContext actx = grid(0).context().authentication().authenticate("test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                AuthorizationContext.context(actx);

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().addUser("test1", "test1");

                        return null;
                    }
                }, IgniteAccessControlException.class, "User management operations are not allowed for user");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().removeUser("test");

                        return null;
                    }
                }, IgniteAccessControlException.class, "User management operations are not allowed for user");

                grid(nodeIdx).context().authentication().updateUser("test", "new_password");

                grid(nodeIdx).context().authentication().updateUser("test", "test");

                // Check error on empty auth context:
                AuthorizationContext.context(null);

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().removeUser("test");

                        return null;
                    }
                }, IgniteAccessControlException.class, "Operation not allowed: authorized context is empty");
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProceedUsersOnJoinNode() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            grid(0).context().authentication().addUser("test0", "test");
            grid(0).context().authentication().addUser("test1", "test");

            int nodeIdx = NODES_COUNT;

            startGrid(nodeIdx);

            AuthorizationContext actx0 = grid(nodeIdx).context().authentication().authenticate("test0", "test");
            AuthorizationContext actx1 = grid(nodeIdx).context().authentication().authenticate("test1", "test");

            assertNotNull(actx0);
            assertEquals("test0", actx0.userName());
            assertNotNull(actx1);
            assertEquals("test1", actx1.userName());
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthenticationInvalidUser() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().authenticate("invalid_name", "test");

                        return null;
                    }
                }, IgniteAccessControlException.class, "The user name or password is incorrect");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().authenticate("test", "invalid_password");

                        return null;
                    }
                }, IgniteAccessControlException.class, "The user name or password is incorrect");
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddUpdateRemoveUser() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int j = 0; j < NODES_COUNT; ++j)
                    checkAddUpdateRemoveUser(grid(i), grid(j));
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateUser() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            grid(0).context().authentication().addUser("test", "test");

            AuthorizationContext actx = grid(0).context().authentication().authenticate("test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int j = 0; j < NODES_COUNT; ++j)
                    checkUpdateUser(actx, grid(i), grid(j));
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateRemoveDoesNotExistsUser() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().updateUser("invalid_name", "test");

                        return null;
                    }
                }, UserManagementException.class, "User doesn't exist");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().removeUser("invalid_name");

                        return null;
                    }
                }, UserManagementException.class, "User doesn't exist");
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddAlreadyExistsUser() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            grid(0).context().authentication().addUser("test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().authentication().addUser("test", "new_passwd");

                        return null;
                    }
                }, UserManagementException.class, "User already exists");
            }
        }
        finally {
            AuthorizationContext.context(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthorizeOnClientDisconnect() throws Exception {
        AuthorizationContext.context(actxDflt);

        grid(CLI_NODE).context().authentication().addUser("test", "test");

        AuthorizationContext.context(null);

        final IgniteInternalFuture stopServersFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 0; i < CLI_NODE; ++i) {
                        Thread.sleep(500);

                        stopGrid(i);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception");
                }
            }
        });

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    while (!stopServersFut.isDone()) {
                        AuthorizationContext actx = grid(CLI_NODE).context().authentication()
                            .authenticate("test", "test");

                        assertNotNull(actx);
                    }

                    return null;
                }
            },
            IgniteCheckedException.class,
            "Client node was disconnected from topology (operation result is unknown)");

        stopServersFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentAddRemove() throws Exception {
        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);
                String user = "test" + usrCnt.getAndIncrement();

                try {
                    for (int i = 0; i < ITERATIONS; ++i) {
                        grid(CLI_NODE).context().authentication().addUser(user, "passwd_" + user);

                        grid(CLI_NODE).context().authentication().removeUser(user);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception");
                }
            }
        }, 10, "user-op");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserPersistence() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            for (int i = 0; i < NODES_COUNT; ++i)
                grid(i).context().authentication().addUser("test" + i, "passwd" + i);

            grid(CLI_NODE).context().authentication().updateUser("ignite", "new_passwd");

            stopAllGrids();

            startGrids(NODES_COUNT - 1);
            startClientGrid(CLI_NODE);

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int usrIdx = 0; usrIdx < NODES_COUNT; ++usrIdx) {
                    AuthorizationContext actx0 = grid(i).context().authentication()
                        .authenticate("test" + usrIdx, "passwd" + usrIdx);

                    assertNotNull(actx0);
                    assertEquals("test" + usrIdx, actx0.userName());
                }

                AuthorizationContext actx = grid(i).context().authentication()
                    .authenticate("ignite", "new_passwd");

                assertNotNull(actx);
                assertEquals("ignite", actx.userName());
            }
        }
        finally {
            AuthorizationContext.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultUserPersistence() throws Exception {
        AuthorizationContext.context(actxDflt);

        try {
            grid(CLI_NODE).context().authentication().addUser("test", "passwd");

            stopAllGrids();

            U.sleep(500);

            startGrids(NODES_COUNT - 1);
            startClientGrid(CLI_NODE);

            for (int i = 0; i < NODES_COUNT; ++i) {
                AuthorizationContext actx = grid(i).context().authentication()
                    .authenticate("ignite", "ignite");

                assertNotNull(actx);
                assertEquals("ignite", actx.userName());

                actx = grid(i).context().authentication()
                    .authenticate("test", "passwd");

                assertNotNull(actx);
                assertEquals("test", actx.userName());

            }
        }
        finally {
            AuthorizationContext.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidUserNamePassword() throws Exception {
        AuthorizationContext.context(actxDflt);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser(null, "test");

                return null;
            }
        }, UserManagementException.class, "User name is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser("", "test");

                return null;
            }
        }, UserManagementException.class, "User name is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser("test", null);

                return null;
            }
        }, UserManagementException.class, "Password is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser("test", "");

                return null;
            }
        }, UserManagementException.class, "Password is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "a");

                return null;
            }
        }, UserManagementException.class, "User name is too long");
    }

    /**
     * @param name User name to check.
     */
    private void checkInvalidUsername(final String name) {

    }

    /**
     * @param passwd User's password to check.
     */
    private void checkInvalidPassword(final String passwd) {
        AuthorizationContext.context(actxDflt);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().authentication().addUser("test", passwd);

                return null;
            }
        }, UserManagementException.class, "Invalid user name");
    }

    /**
     * @param createNode Node to execute create operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkAddUpdateRemoveUser(IgniteEx createNode, IgniteEx authNode) throws Exception {
        createNode.context().authentication().addUser("test", "test");

        AuthorizationContext newActx = authNode.context().authentication().authenticate("test", "test");

        assertNotNull(newActx);
        assertEquals("test", newActx.userName());

        createNode.context().authentication().updateUser("test", "newpasswd");

        newActx = authNode.context().authentication().authenticate("test", "newpasswd");

        assertNotNull(newActx);
        assertEquals("test", newActx.userName());

        createNode.context().authentication().removeUser("test");
    }

    /**
     * @param actx Authorization context.
     * @param updNode Node to execute update operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkUpdateUser(AuthorizationContext actx, IgniteEx updNode, IgniteEx authNode) throws Exception {
        String newPasswd = randomString(16);

        updNode.context().authentication().updateUser("test", newPasswd);

        AuthorizationContext actxNew = authNode.context().authentication().authenticate("test", newPasswd);

        assertNotNull(actxNew);
        assertEquals("test", actxNew.userName());
    }
}
