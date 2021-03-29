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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.commons.lang3.StringUtils.repeat;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;

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
            final int nodeIdx = i;

            assertThrows(() -> dropUser(grid(nodeIdx), secCtxDflt, "ignite"),
                IgniteAccessControlException.class, "Default user cannot be removed");

            assertNotNull(authenticate(grid(nodeIdx), "ignite", "ignite"));
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
            final int nodeIdx = i;

            assertThrows(() -> createUser(grid(nodeIdx), secCtx, "test1", "test1"),
                IgniteAccessControlException.class, "User management operations are not allowed for user");

            assertThrows(() -> dropUser(grid(nodeIdx), secCtx, "test"),
                IgniteAccessControlException.class, "User management operations are not allowed for user");

            alterUserPassword(grid(nodeIdx), secCtx, "test", "new_password");

            alterUserPassword(grid(nodeIdx), secCtx, "test", "test");

            assertThrows(() -> dropUser(grid(nodeIdx), null, "test"),
                IgniteAccessControlException.class, "Operation not allowed: security context is empty");
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProceedUsersOnJoinNode() throws Exception {
        createUser(grid(0), secCtxDflt, "test0", "test");
        createUser(grid(0), secCtxDflt, "test1", "test");

        int nodeIdx = NODES_COUNT;

        startGrid(nodeIdx);

        SecurityContext secCtx0 = authenticate(grid(nodeIdx), "test0", "test");
        SecurityContext secCtx1 = authenticate(grid(nodeIdx), "test1", "test");

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
            final int nodeIdx = i;

            assertThrows(() -> authenticate(grid(nodeIdx), "invalid_name", "test"),
                IgniteAccessControlException.class, "The user name or password is incorrect");

            assertThrows(() -> authenticate(grid(nodeIdx), "test", "invalid_password"),
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
            final int nodeIdx = i;

            assertThrows(() -> alterUserPassword(grid(nodeIdx), secCtxDflt, "invalid_name", "test"),
                UserManagementException.class, "User doesn't exist");

            assertThrows(() -> dropUser(grid(nodeIdx), secCtxDflt, "invalid_name"),
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
        createUser(grid(CLI_NODE), secCtxDflt, "test", "test");

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
                    assertNotNull(authenticate(grid(CLI_NODE), "test", "test"));
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
                   createUser(grid(CLI_NODE), secCtxDflt, user, "passwd_" + user);

                   dropUser(grid(CLI_NODE), secCtxDflt, user);
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
            for (int usrIdx = 0; usrIdx < NODES_COUNT; ++usrIdx) {
                SecurityContext secCtx0 = authenticate(grid(i), "test" + usrIdx, "passwd" + usrIdx);

                assertNotNull(secCtx0);
                assertEquals("test" + usrIdx, secCtx0.subject().login());
            }

            SecurityContext secCtz = authenticate(grid(i), "ignite", "new_passwd");

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
            SecurityContext secCtx = authenticate(grid(i), "ignite", "ignite");

            assertNotNull(secCtx);
            assertEquals("ignite", secCtx.subject().login());

            secCtx = authenticate(grid(i), "test", "passwd");

            assertNotNull(secCtx);
            assertEquals("test", secCtx.subject().login());

        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidUserNamePassword() throws Exception {
        assertThrows(() -> createUser(grid(CLI_NODE), null, null, "test"),
            UserManagementException.class, "User name is empty");

        assertThrows(() -> createUser(grid(CLI_NODE), null, "", "test"),
            UserManagementException.class, "User name is empty");

        assertThrows(() -> createUser(grid(CLI_NODE), null, "test", null),
            UserManagementException.class, "Password is empty");

        assertThrows(() -> createUser(grid(CLI_NODE), null, "test", ""),
            UserManagementException.class, "Password is empty");

        assertThrows(() -> createUser(grid(CLI_NODE), null, repeat('a', 60), "a"),
            UserManagementException.class, "User name is too long");
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

    /** */
    public static SecurityContext authenticate(IgniteEx ignite, String login, String pwd) throws IgniteCheckedException {
        AuthenticationContext authCtx = new AuthenticationContext();

        authCtx.credentials(new SecurityCredentials(login, pwd));

        return ignite.context().security().authenticate(authCtx);
    }

    /** Creates user with specified login and password. */
    public static void createUser(IgniteEx ignite, SecurityContext ctx, String login, String pwd)
        throws IgniteCheckedException {
        withSecurityContext(ignite, ctx, ign ->
            ign.context().security().createUser(login, new UserOptions().password(pwd)));
    }

    /** Alters password of the user with the specified login. */
    public static void alterUserPassword(IgniteEx ignite, SecurityContext ctx, String login, String newPwd)
        throws IgniteCheckedException {
        withSecurityContext(ignite, ctx, ign ->
            ign.context().security().alterUser(login, new UserOptions().password(newPwd)));
    }

    /** Drops the user with specified login. */
    public static void dropUser(IgniteEx ignite, SecurityContext ctx, String login) throws IgniteCheckedException {
        withSecurityContext(ignite, ctx, ign -> ign.context().security().dropUser(login));
    }

    /**
     * @param ignite Ignite instance to perform the operation on.
     * @param ctx Security context in which operation will be executed.
     * @param op Operation to execute.
     */
    public static void withSecurityContext(
        IgniteEx ignite,
        SecurityContext ctx,
        IgniteThrowableConsumer<IgniteEx> op
    ) throws IgniteCheckedException {
        IgniteSecurity security = ignite.context().security();

        if (ctx != null) {
            try (OperationSecurityContext ignored = security.withContext(ctx)) {
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
