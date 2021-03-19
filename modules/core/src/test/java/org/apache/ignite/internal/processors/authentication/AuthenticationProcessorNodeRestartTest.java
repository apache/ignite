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

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.alterUserPassword;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.createUser;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.dropUser;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;

/**
 * Test for {@link IgniteAuthenticationProcessor} on unstable topology.
 */
public class AuthenticationProcessorNodeRestartTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_COUNT = 4;

    /** Nodes restarts count. */
    private static final int RESTARTS = 10;

    /** Client node. */
    private static final int CLI_NODE = NODES_COUNT - 1;

    /** Security context for default user. */
    private SecurityContext secCtxDflt;

    /** Random. */
    private static final Random RND = new Random(System.currentTimeMillis());

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
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-7472")
    @Test
    public void testConcurrentAddUpdateRemoveNodeRestartCoordinator() throws Exception {
        final IgniteInternalFuture restartFut = restartCoordinator();

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            String user = "test" + usrCnt.getAndIncrement();

            try {
                int state = 0;
                while (!restartFut.isDone()) {
                    try {
                        switch (state) {
                            case 0:
                                createUser(grid(CLI_NODE), secCtxDflt, user, "passwd_" + user);

                                break;

                            case 1:
                                alterUserPassword(grid(CLI_NODE), secCtxDflt, user, "new_passwd_" + user);

                                break;

                            case 2:
                                dropUser(grid(CLI_NODE), secCtxDflt, user);

                                break;

                            default:
                                fail("Invalid state: " + state);
                        }

                        state = ++state > 2 ? 0 : state;
                    }
                    catch (UserManagementException e) {
                        U.error(log, e);
                        fail("Unexpected exception on user operation");
                    }
                    catch (IgniteCheckedException e) {
                        // Reconnect
                        U.error(log, e);
                    }
                }
            }
            catch (Exception e) {
                U.error(log, "Unexpected exception on concurrent add/remove: " + user, e);
                fail();
            }
        }, 10, "user-op");

        restartFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentAuthorize() throws Exception {
        final int testUsersCnt = 10;

        for (int i = 0; i < testUsersCnt; ++i)
            createUser(grid(CLI_NODE), secCtxDflt, "test" + i, "passwd_test" + i);

        final IgniteInternalFuture restartFut = GridTestUtils.runAsync(() -> {
            try {
                for (int i = 0; i < RESTARTS; ++i) {
                    int nodeIdx = RND.nextInt(NODES_COUNT - 1);

                    stopGrid(nodeIdx);

                    U.sleep(500);

                    startGrid(nodeIdx);

                    U.sleep(500);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on server restart: " + e.getMessage());
            }
        });

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            String user = "test" + usrCnt.getAndIncrement();

            try {
                while (!restartFut.isDone())
                    assertNotNull(authenticate(grid(CLI_NODE), user, "passwd_" + user));
            }
            catch (ClusterTopologyCheckedException ignored) {
                // No-op.
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception: " + e.getMessage());
            }
        }, testUsersCnt, "user-op");

        restartFut.get();
    }

    /**
     * @return Future.
     */
    private IgniteInternalFuture restartCoordinator() {
        return GridTestUtils.runAsync(() -> {
            try {
                int restarts = 0;

                while (restarts < RESTARTS) {
                    for (int i = 0; i < CLI_NODE; ++i, ++restarts) {
                        if (restarts >= RESTARTS)
                            break;

                        stopGrid(i);

                        U.sleep(500);

                        startGrid(i);

                        U.sleep(500);
                    }
                }
            }
            catch (Exception e) {
                U.error(log, "Unexpected exception on coordinator restart", e);
                fail();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test1kUsersNodeRestartServer() throws Exception {
        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            try {
                while (usrCnt.get() < 200) {
                    String user = "test" + usrCnt.getAndIncrement();

                    if (log.isDebugEnabled())
                        log.debug("+++ CREATE  " + user);

                    createUser(grid(0), secCtxDflt, user, "init");
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on add / remove");
            }
        }, 3, "user-op");

        usrCnt.set(0);

        GridTestUtils.runMultiThreaded(() -> {
            try {
                while (usrCnt.get() < 200) {
                    String user = "test" + usrCnt.getAndIncrement();

                    if (log.isDebugEnabled())
                        log.debug("+++ ALTER " + user);

                    alterUserPassword(grid(0), secCtxDflt, user, "passwd_" + user);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on add / remove");
            }
        }, 3, "user-op");

        if (log.isDebugEnabled())
            log.debug("+++ STOP");

        stopGrid(0, true);

        U.sleep(1000);

        if (log.isDebugEnabled())
            log.debug("+++ START");

        startGrid(0);

        authenticate(grid(0), "ignite", "ignite");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-14301")
    public void testConcurrentAddUpdateRemoveNodeRestartServer() throws Exception {
        IgniteInternalFuture restartFut = loopServerRestarts();

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            String user = "test" + usrCnt.getAndIncrement();

            try {
                while (!restartFut.isDone()) {
                    createUser(grid(CLI_NODE), secCtxDflt, user, "init");

                    alterUserPassword(grid(CLI_NODE), secCtxDflt, user, "passwd_" + user);

                    dropUser(grid(CLI_NODE), secCtxDflt, user);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on add / remove");
            }
        }, 10, "user-op");

        restartFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentFailedOperationNodeRestartServer() throws Exception {
        IgniteInternalFuture restartFut = loopServerRestarts();

        createUser(grid(CLI_NODE), secCtxDflt, "test", "test");

        GridTestUtils.runMultiThreaded(() -> {
            try {
                while (!restartFut.isDone()) {
                    GridTestUtils.assertThrows(log, () -> {
                        createUser(grid(CLI_NODE), secCtxDflt, "test", "test");

                        return null;
                    }, UserManagementException.class, "User already exists");
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected error on failed operation");
            }
        }, 10, "user-op");

        restartFut.get();
    }

    /** */
    private IgniteInternalFuture loopServerRestarts() {
        return GridTestUtils.runAsync(() -> {
            try {
                for (int i = 0; i < RESTARTS; ++i) {
                    stopGrid(1);

                    U.sleep(500);

                    startGrid(1);

                    U.sleep(500);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on server restart: " + e.getMessage());
            }
        });
    }
}
