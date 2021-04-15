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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.security.IgniteSecurity;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.processors.security.UserOptions;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.lang.IgniteThrowableRunner;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.AuthenticationContext;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.ignite.internal.processors.authentication.User.DEFAULT_USER_NAME;
import static org.apache.ignite.plugin.security.SecurityPermission.ALTER_USER;
import static org.apache.ignite.plugin.security.SecurityPermission.CREATE_USER;
import static org.apache.ignite.plugin.security.SecurityPermission.DROP_USER;
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

        secCtxDflt = authenticate(grid(0), DEFAULT_USER_NAME, "ignite");

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
        // Change from all nodes
        for (int nodeIdx = 0; nodeIdx < NODES_COUNT; ++nodeIdx) {
            alterUserPassword(grid(nodeIdx), secCtxDflt, "ignite", "ignite" + nodeIdx);

            // Check each change from all nodes
            for (int i = 0; i < NODES_COUNT; ++i) {
                SecurityContext secCtx = authenticate(grid(i), "ignite", "ignite" + nodeIdx);

                assertNotNull(secCtx);
                assertEquals("ignite", secCtx.subject().login());
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRemoveDefault() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            assertThrows(() -> authorizeUserOperation(srv, secCtxDflt, DEFAULT_USER_NAME, DROP_USER),
                SecurityException.class, "Default user cannot be removed");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserManagementPermission() throws Exception {
        createUser(grid(0), secCtxDflt, "test", "test");

        final SecurityContext secCtx = authenticate(grid(0), "test", "test");

        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            assertThrows(() -> authorizeUserOperation(srv, secCtx, "test1", CREATE_USER), SecurityException.class,
                "User management operations are not allowed for user");

            assertThrows(() -> authorizeUserOperation(srv, secCtx, "test", DROP_USER), SecurityException.class,
                "User management operations are not allowed for user");

            authorizeUserOperation(srv, secCtx, "test", ALTER_USER);

            authorizeUserOperation(srv, secCtx, "test", ALTER_USER);

            assertThrows(() -> authorizeUserOperation(srv, null, "test", DROP_USER), SecurityException.class,
                "User management operations initiated on behalf of the Ignite node are not supported");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProceedUsersOnJoinNode() throws Exception {
        createUser(grid(0), secCtxDflt, "test0", "test");
        createUser(grid(0), secCtxDflt, "test1", "test");

        IgniteEx srv = startGrid(NODES_COUNT);

        SecurityContext secCtx0 = authenticate(srv, "test0", "test");
        SecurityContext secCtx1 = authenticate(srv, "test1", "test");

        assertNotNull(secCtx0);
        assertEquals("test0", secCtx0.subject().login());
        assertNotNull(secCtx1);
        assertEquals("test1", secCtx1.subject().login());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthenticationInvalidUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            assertThrows(() -> authenticate(srv, "invalid_name", "test"),
                IgniteAccessControlException.class, "The user name or password is incorrect");

            assertThrows(() -> authenticate(srv, "test", "invalid_password"),
                IgniteAccessControlException.class, "The user name or password is incorrect");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddUpdateRemoveUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            for (int j = 0; j < NODES_COUNT; ++j)
                checkAddUpdateRemoveUser(grid(i), grid(j));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateUser() throws Exception {
            createUser(grid(0), secCtxDflt, "test", "test");

            SecurityContext secCtx = authenticate(grid(0), "test", "test");

            for (int i = 0; i < NODES_COUNT; ++i) {
                for (int j = 0; j < NODES_COUNT; ++j)
                    checkUpdateUser(secCtx, grid(i), grid(j));
            }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateRemoveDoesNotExistsUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            assertThrows(() -> alterUserPassword(srv, secCtxDflt, "invalid_name", "test"),
                UserManagementException.class, "User doesn't exist");

            assertThrows(() -> dropUser(srv, secCtxDflt, "invalid_name"),
                UserManagementException.class, "User doesn't exist");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAddAlreadyExistsUser() throws Exception {
        createUser(grid(0), secCtxDflt, "test", "test");

        for (int i = 0; i < NODES_COUNT; ++i) {
            final int nodeIdx = i;

            assertThrows(() -> createUser(grid(nodeIdx), secCtxDflt, "test", "new_passwd"),
                UserManagementException.class, "User already exists");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAuthorizeOnClientDisconnect() throws Exception {
        IgniteEx cli = grid(CLI_NODE);

        createUser(cli, secCtxDflt, "test", "test");

        final IgniteInternalFuture stopServersFut = GridTestUtils.runAsync(() -> {
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
        });

        assertThrows(() -> {
                while (!stopServersFut.isDone())
                    assertNotNull(authenticate(cli, "test", "test"));
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

        GridTestUtils.runMultiThreaded(() -> {
            String user = "test" + usrCnt.getAndIncrement();

            try {
                for (int i = 0; i < ITERATIONS; ++i) {
                    IgniteEx cli = grid(CLI_NODE);

                   createUser(cli, secCtxDflt, user, "passwd_" + user);

                   dropUser(cli, secCtxDflt, user);
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                fail("Unexpected exception");
            }
        }, 10, "user-op");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUserPersistence() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i)
            createUser(grid(i), secCtxDflt, "test" + i, "passwd" + i);

        alterUserPassword(grid(CLI_NODE), secCtxDflt, "ignite", "new_passwd");

        stopAllGrids();

        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            for (int usrIdx = 0; usrIdx < NODES_COUNT; ++usrIdx) {
                SecurityContext secCtx0 = authenticate(srv, "test" + usrIdx, "passwd" + usrIdx);

                assertNotNull(secCtx0);
                assertEquals("test" + usrIdx, secCtx0.subject().login());
            }

            SecurityContext secCtz = authenticate(srv, "ignite", "new_passwd");

            assertNotNull(secCtz);
            assertEquals("ignite", secCtxDflt.subject().login());
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultUserPersistence() throws Exception {
        createUser(grid(CLI_NODE), secCtxDflt, "test", "passwd");

        stopAllGrids();

        U.sleep(500);

        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        for (int i = 0; i < NODES_COUNT; ++i) {
            IgniteEx srv = grid(i);

            SecurityContext secCtx = authenticate(srv, "ignite", "ignite");

            assertNotNull(secCtx);
            assertEquals("ignite", secCtx.subject().login());

            secCtx = authenticate(srv, "test", "passwd");

            assertNotNull(secCtx);
            assertEquals("test", secCtx.subject().login());

        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidUserNamePassword() throws Exception {
        IgniteEx cli = grid(CLI_NODE);

        assertThrows(() -> createUser(cli, null, null, "test"),
            UserManagementException.class, "User name is empty");

        assertThrows(() -> createUser(cli, null, "", "test"),
            UserManagementException.class, "User name is empty");

        assertThrows(() -> createUser(cli, null, "test", null),
            UserManagementException.class, "Password is empty");

        assertThrows(() -> createUser(cli, null, "test", ""),
            UserManagementException.class, "Password is empty");

        assertThrows(() -> createUser(cli, null, repeat('a', 60), "a"),
            UserManagementException.class, "User name is too long");
    }

    /** Test the ability to obtain the security context ot an authenticated user on the remote server node. */
    @Test
    public void testRemoteNodeSecurityContext() throws Exception {
        createUser(grid(CLI_NODE), secCtxDflt, "test", "pwd");

        SecuritySubject subj = authenticate(grid(0), "test", "pwd").subject();

        for (int i = 1; i < NODES_COUNT - 1; i++) {
            IgniteSecurity security = ignite(i).context().security();

            try (OperationSecurityContext ignored = security.withContext(subj.id())) {
                SecuritySubject rmtSubj = security.securityContext().subject();

                assertEquals(subj.id(), rmtSubj.id());
                assertEquals(subj.login(), rmtSubj.login());
                assertEquals(subj.type(), rmtSubj.type());
            }
        }
    }

    /**
     * @param passwd User's password to check.
     */
    private void checkInvalidPassword(final String passwd) {
        assertThrows(() -> createUser(grid(CLI_NODE), secCtxDflt, "test", passwd),
            UserManagementException.class, "Invalid user name");
    }

    /**
     * @param createNode Node to execute create operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkAddUpdateRemoveUser(IgniteEx createNode, IgniteEx authNode) throws Exception {
        createUser(createNode, secCtxDflt, "test", "test");

        SecurityContext newSecCtx = authenticate(authNode, "test", "test");

        assertNotNull(newSecCtx);
        assertEquals("test", newSecCtx.subject().login());

        alterUserPassword(createNode, secCtxDflt, "test", "newpasswd");

        newSecCtx = authenticate(authNode, "test", "newpasswd");

        assertNotNull(newSecCtx);
        assertEquals("test", newSecCtx.subject().login());

        dropUser(createNode, secCtxDflt, "test");
    }

    /**
     * @param secCtx Security context.
     * @param updNode Node to execute update operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkUpdateUser(SecurityContext secCtx, IgniteEx updNode, IgniteEx authNode) throws Exception {
        String newPasswd = randomString(16);

        alterUserPassword(updNode, secCtx, "test", newPasswd);

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

    /** Creates user with specified login and password. */
    public static void createUser(IgniteEx ignite, SecurityContext secCtx, String login, String pwd)
        throws IgniteCheckedException {
        withSecurityContext(ignite, secCtx, ign ->
            ign.context().security().createUser(login, new UserOptions().password(pwd)));
    }

    /** Alters password of the user with the specified login. */
    public static void alterUserPassword(IgniteEx ignite, SecurityContext secCtx, String login, String newPwd)
        throws IgniteCheckedException {
        withSecurityContext(ignite, secCtx, ign ->
            ign.context().security().alterUser(login, new UserOptions().password(newPwd)));
    }

    /** Drops the user with specified login. */
    public static void dropUser(IgniteEx ignite, SecurityContext secCtx, String login) throws IgniteCheckedException {
        withSecurityContext(ignite, secCtx, ign -> ign.context().security().dropUser(login));
    }

    /** Authorizes user management operation. */
    private void authorizeUserOperation(IgniteEx ignite, SecurityContext secCtx, String login, SecurityPermission perm)
        throws IgniteCheckedException {
        withSecurityContext(ignite, secCtx, ign -> ign.context().security().authorize(login, perm));
    }

    /**
     * @param ignite Ignite instance to perform the operation on.
     * @param secCtx Security context in which operation will be executed.
     * @param op Operation to execute.
     */
    public static void withSecurityContext(IgniteEx ignite, SecurityContext secCtx, IgniteThrowableConsumer<IgniteEx> op)
        throws IgniteCheckedException {
        if (secCtx != null) {
            try (OperationSecurityContext ignored = ignite.context().security().withContext(secCtx)) {
                op.accept(ignite);
            }
        }
        else
            op.accept(ignite);
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    static void assertThrows(IgniteThrowableRunner r, Class<? extends Throwable> errCls, String msg) {
        GridTestUtils.assertThrows(log, () -> {
            r.run();

            return null;
        }, errCls, msg);
    }
}
