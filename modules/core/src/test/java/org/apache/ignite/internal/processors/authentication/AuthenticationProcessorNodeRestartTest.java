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
import org.apache.ignite.cluster.ClusterState;
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

import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;
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

    /** Authorization context for default user. */
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

        recreateDefaultDb();

        startGrids(NODES_COUNT - 1);
        startClientGrid(CLI_NODE);

        grid(0).cluster().state(ClusterState.ACTIVE);

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

        withSecurityContextOnAllNodes(secCtxDflt);

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            withSecurityContextOnAllNodes(secCtxDflt);

            String user = "test" + usrCnt.getAndIncrement();

            try {
                int state = 0;
                while (!restartFut.isDone()) {
                    try {
                        switch (state) {
                            case 0:
                                grid(CLI_NODE).context().security().createUser(user, ("passwd_" + user).toCharArray());

                                break;

                            case 1:
                                grid(CLI_NODE).context().security().alterUser(user, ("new_passwd_" + user).toCharArray());

                                break;

                            case 2:
                                grid(CLI_NODE).context().security().dropUser(user);

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

        withSecurityContextOnAllNodes(secCtxDflt);

        for (int i = 0; i < testUsersCnt; ++i)
            grid(CLI_NODE).context().security().createUser("test" + i, ("passwd_test" + i).toCharArray());

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
                while (!restartFut.isDone()) {
                    SecurityContext secCtx = authenticate(grid(CLI_NODE), user, "passwd_" + user);

                    assertNotNull(secCtx);
                }
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
            withSecurityContextOnAllNodes(secCtxDflt);

            try {
                while (usrCnt.get() < 200) {
                    String user = "test" + usrCnt.getAndIncrement();

                    System.out.println("+++ CREATE  " + user);
                    grid(0).context().security().createUser(user, "init".toCharArray());
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on add / remove");
            }
        }, 3, "user-op");

        usrCnt.set(0);

        GridTestUtils.runMultiThreaded(() -> {
            withSecurityContextOnAllNodes(secCtxDflt);

            try {
                while (usrCnt.get() < 200) {
                    String user = "test" + usrCnt.getAndIncrement();

                    System.out.println("+++ ALTER " + user);

                    grid(0).context().security().alterUser(user, ("passwd_" + user).toCharArray());
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                fail("Unexpected exception on add / remove");
            }
        }, 3, "user-op");

        System.out.println("+++ STOP");
        stopGrid(0, true);

        U.sleep(1000);

        System.out.println("+++ START");
        startGrid(0);

        authenticate(grid(0), "ignite", "ignite");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentAddUpdateRemoveNodeRestartServer() throws Exception {
        IgniteInternalFuture restartFut = loopServerRestarts();

        withSecurityContextOnAllNodes(secCtxDflt);

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(() -> {
            withSecurityContextOnAllNodes(secCtxDflt);

            String user = "test" + usrCnt.getAndIncrement();

            try {
                while (!restartFut.isDone()) {
                    grid(CLI_NODE).context().security().createUser(user, "init".toCharArray());

                    grid(CLI_NODE).context().security().alterUser(user, ("passwd_" + user).toCharArray());

                    grid(CLI_NODE).context().security().dropUser(user);
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

        withSecurityContextOnAllNodes(secCtxDflt);

        grid(CLI_NODE).context().security().createUser("test", "test".toCharArray());

        GridTestUtils.runMultiThreaded(() -> {
            withSecurityContextOnAllNodes(secCtxDflt);

            try {
                while (!restartFut.isDone()) {
                    GridTestUtils.assertThrows(log, () -> {
                        grid(CLI_NODE).context().security().createUser("test", "test".toCharArray());

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
