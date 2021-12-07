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

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.plugin.security.SecuritySubjectType.REMOTE_CLIENT;

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

    /** Security context for default user. */
    protected SecurityContext secCtxDflt;

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

        secCtxDflt = authenticate(grid(0), DFAULT_USER_NAME, "ignite");

        assertNotNull(secCtxDflt);
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
            SecurityContext secCtx = authenticate(grid(i), "ignite", "ignite");

            assertNotNull(secCtx);
            assertEquals("ignite", secCtx.subject().login());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultUserUpdate() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            // Change from all nodes
            for (int nodeIdx = 0; nodeIdx < NODES_COUNT; ++nodeIdx) {
                grid(nodeIdx).context().security().alterUser("ignite", ("ignite" + nodeIdx).toCharArray());

                // Check each change from all nodes
                for (int i = 0; i < NODES_COUNT; ++i) {
                    SecurityContext secCtx = authenticate(grid(i), "ignite", "ignite" + nodeIdx);

                    assertNotNull(secCtx);
                    assertEquals("ignite", secCtx.subject().login());
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveDefault() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().dropUser("ignite");

                        return null;
                    }
                }, IgniteAccessControlException.class, "Default user cannot be removed");

                assertNotNull(authenticate(grid(0), "ignite", "ignite"));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserManagementPermission() throws Exception {
        try (AutoCloseable nodeCtxsHnd = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(0).context().security().createUser("test", "test".toCharArray());

            final SecurityContext secCtx = authenticate(grid(0), "test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                AutoCloseable innerNodeCtxsHnd = withSecurityContextOnAllNodes(secCtx);

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().createUser("test1", "test1".toCharArray());

                        return null;
                    }
                }, IgniteAccessControlException.class, "User management operations are not allowed for user");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().dropUser("test");

                        return null;
                    }
                }, IgniteAccessControlException.class, "User management operations are not allowed for user");

                grid(nodeIdx).context().security().alterUser("test", "new_password".toCharArray());

                grid(nodeIdx).context().security().alterUser("test", "test".toCharArray());

                // Check error on empty auth context:
                innerNodeCtxsHnd.close();
                nodeCtxsHnd.close();

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().dropUser("test");

                        return null;
                    }
                }, IgniteAccessControlException.class,
                "User management operations initiated on behalf of the Ignite node are not expected.");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProceedUsersOnJoinNode() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(0).context().security().createUser("test0", "test".toCharArray());
            grid(0).context().security().createUser("test1", "test".toCharArray());

            int nodeIdx = NODES_COUNT;

            startGrid(nodeIdx);

            SecurityContext secCtx0 = authenticate(grid(nodeIdx), "test0", "test");
            SecurityContext secCtx1 = authenticate(grid(nodeIdx), "test1", "test");

            assertNotNull(secCtx0);
            assertEquals("test0", secCtx0.subject().login());
            assertNotNull(secCtx1);
            assertEquals("test1", secCtx1.subject().login());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthenticationInvalidUser() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        authenticate(grid(nodeIdx), "invalid_name", "test");

                        return null;
                    }
                }, IgniteAccessControlException.class, "The user name or password is incorrect");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        authenticate(grid(nodeIdx), "test", "invalid_password");

                        return null;
                    }
                }, IgniteAccessControlException.class, "The user name or password is incorrect");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddUpdateRemoveUser() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int j = 0; j < NODES_COUNT; ++j)
                    checkAddUpdateRemoveUser(grid(i), grid(j));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateUser() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(0).context().security().createUser("test", "test".toCharArray());

            SecurityContext secCtx = authenticate(grid(0), "test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int j = 0; j < NODES_COUNT; ++j)
                    checkUpdateUser(secCtx, grid(i), grid(j));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateRemoveDoesNotExistsUser() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().alterUser("invalid_name", "test".toCharArray());

                        return null;
                    }
                }, UserManagementException.class, "User doesn't exist");

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().dropUser("invalid_name");

                        return null;
                    }
                }, UserManagementException.class, "User doesn't exist");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddAlreadyExistsUser() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(0).context().security().createUser("test", "test".toCharArray());

            for (int i = 0; i < NODES_COUNT; ++i) {
                final int nodeIdx = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        grid(nodeIdx).context().security().createUser("test", "new_passwd".toCharArray());

                        return null;
                    }
                }, UserManagementException.class, "User already exists");
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthorizeOnClientDisconnect() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(CLI_NODE).context().security().createUser("test", "test".toCharArray());
        }

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
                        SecurityContext secCtx = authenticate(grid(CLI_NODE), "test", "test");

                        assertNotNull(secCtx);
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
                withSecurityContextOnAllNodes(secCtxDflt);
                String user = "test" + usrCnt.getAndIncrement();

                try {
                    for (int i = 0; i < ITERATIONS; ++i) {
                        grid(CLI_NODE).context().security().createUser(user, ("passwd_" + user).toCharArray());

                        grid(CLI_NODE).context().security().dropUser(user);
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
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            for (int i = 0; i < NODES_COUNT; ++i)
                grid(i).context().security().createUser("test" + i, ("passwd" + i).toCharArray());

            grid(CLI_NODE).context().security().alterUser("ignite", "new_passwd".toCharArray());

            stopAllGrids();

            startGrids(NODES_COUNT - 1);
            startClientGrid(CLI_NODE);

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int usrIdx = 0; usrIdx < NODES_COUNT; ++usrIdx) {
                    SecurityContext secCtx0 = authenticate(grid(i), "test" + usrIdx, "passwd" + usrIdx);

                    assertNotNull(secCtx0);
                    assertEquals("test" + usrIdx, secCtx0.subject().login());
                }

                SecurityContext secCtx = authenticate(grid(i), "ignite", "new_passwd");

                assertNotNull(secCtx);
                assertEquals("ignite", secCtx.subject().login());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultUserPersistence() throws Exception {
        try (AutoCloseable ignored = withSecurityContextOnAllNodes(secCtxDflt)) {
            grid(CLI_NODE).context().security().createUser("test", "passwd".toCharArray());

            stopAllGrids();

            U.sleep(500);

            startGrids(NODES_COUNT - 1);
            startClientGrid(CLI_NODE);

            for (int i = 0; i < NODES_COUNT; ++i) {
                SecurityContext secCtx = authenticate(grid(i), "ignite", "ignite");

                assertNotNull(secCtx);
                assertEquals("ignite", secCtx.subject().login());

                secCtx = authenticate(grid(i), "test", "passwd");

                assertNotNull(secCtx);
                assertEquals("test", secCtx.subject().login());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidUserNamePassword() throws Exception {
        withSecurityContextOnAllNodes(secCtxDflt);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser(null, "test".toCharArray());

                return null;
            }
        }, UserManagementException.class, "User name is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser("", "test".toCharArray());

                return null;
            }
        }, UserManagementException.class, "User name is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser("test", null);

                return null;
            }
        }, UserManagementException.class, "Password is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser("test", "".toCharArray());

                return null;
            }
        }, UserManagementException.class, "Password is empty");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "a".toCharArray());

                return null;
            }
        }, UserManagementException.class, "User name is too long");
    }

    /**
     * @param name User name to check.
     */
    private void checkInvalidUsername(final String name) {

    }

    /** Test the ability to obtain the security context ot an authenticated user on the remote server node. */
    @Test
    public void testRemoteNodeSecurityContext() throws Exception {
        try (OperationSecurityContext ignored = grid(CLI_NODE).context().security().withContext(secCtxDflt)) {
            grid(CLI_NODE).context().security().createUser("test", "pwd".toCharArray());
        }

        SecuritySubject subj = authenticate(grid(0), "test", "pwd").subject();

        for (int i = 1; i < NODES_COUNT; i++) {
            IgniteSecurity security = ignite(i).context().security();

            try (OperationSecurityContext ignored = security.withContext(subj.id())) {
                SecuritySubject rmtSubj = security.securityContext().subject();

                assertEquals(subj.id(), rmtSubj.id());
                assertEquals(i != CLI_NODE ? subj.login() : null, rmtSubj.login());
                assertEquals(subj.type(), rmtSubj.type());
            }
        }
    }

    /**
     * @param passwd User's password to check.
     */
    private void checkInvalidPassword(final String passwd) {
        withSecurityContextOnAllNodes(secCtxDflt);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid(CLI_NODE).context().security().createUser("test", passwd.toCharArray());

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
        createNode.context().security().createUser("test", "test".toCharArray());

        SecurityContext newSecCtx = authenticate(authNode, "test", "test");

        assertNotNull(newSecCtx);
        assertEquals("test", newSecCtx.subject().login());

        createNode.context().security().alterUser("test", "newpasswd".toCharArray());

        newSecCtx = authenticate(authNode, "test", "newpasswd");

        assertNotNull(newSecCtx);
        assertEquals("test", newSecCtx.subject().login());

        createNode.context().security().dropUser("test");
    }

    /**
     * @param secCtx Security context.
     * @param updNode Node to execute update operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkUpdateUser(SecurityContext secCtx, IgniteEx updNode, IgniteEx authNode) throws Exception {
        String newPasswd = randomString(16);

        updNode.context().security().alterUser("test", newPasswd.toCharArray());

        SecurityContext secCtxNew = authenticate(authNode, "test", newPasswd);

        assertNotNull(secCtxNew);
        assertEquals("test", secCtxNew.subject().login());
    }

    /** Authenticates user on the specified node. */
    public static SecurityContext authenticate(IgniteEx ignite, String login, String pwd) throws IgniteCheckedException {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.credentials(new SecurityCredentials(login, pwd));
        authCtx.subjectType(REMOTE_CLIENT);

        return ignite.context().security().authenticate(authCtx);
    }

    /**
     * Sets specified security context on all nodes.
     *
     * @return Holder of current security contexts. If closed, all security context will bew restored.
     */
    public static AutoCloseable withSecurityContextOnAllNodes(SecurityContext ctx) {
        List<OperationSecurityContext> oldSecCtxs = new ArrayList<>();

        for (Ignite node : G.allGrids())
            oldSecCtxs.add(((IgniteEx)node).context().security().withContext(ctx));

        return new AutoCloseable() {
            @Override public void close() throws Exception {
               oldSecCtxs.forEach(OperationSecurityContext::close);
            }
        };
    }
}
