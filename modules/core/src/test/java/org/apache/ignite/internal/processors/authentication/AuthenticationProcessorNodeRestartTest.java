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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link IgniteAuthenticationProcessor} on unstable topology.
 */
public class AuthenticationProcessorNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Nodes count. */
    private static final int NODES_COUNT = 4;

    /** Nodes restarts count. */
    private static final int RESTARTS = 10;

    /** Client node. */
    protected static final int CLI_NODE = NODES_COUNT - 1;

    /** Authorization context for default user. */
    protected AuthorizationContext actxDflt;

    /** Random. */
    private static final Random RND = new Random(System.currentTimeMillis());

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) == CLI_NODE)
            cfg.setClientMode(true);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(200L * 1024 * 1024)
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", true);

        startGrids(NODES_COUNT);

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
    public void testConcurrentAddUpdateRemoveNodeRestartCoordinator() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-7472");

        final IgniteInternalFuture restartFut = restartCoordinator();

        AuthorizationContext.context(actxDflt);

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);

                String user = "test" + usrCnt.getAndIncrement();

                try {
                    int state = 0;
                    while (!restartFut.isDone()) {
                        try {
                            switch (state) {
                                case 0:
                                    grid(CLI_NODE).context().authentication().addUser(user, "passwd_" + user);

                                    break;

                                case 1:
                                    grid(CLI_NODE).context().authentication().updateUser(user, "new_passwd_" + user);

                                    break;

                                case 2:
                                    grid(CLI_NODE).context().authentication().removeUser(user);

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
            }
        }, 10, "user-op");

        restartFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentAuthorize() throws Exception {
        final int testUsersCnt = 10;

        AuthorizationContext.context(actxDflt);

        for (int i = 0; i < testUsersCnt; ++i)
            grid(CLI_NODE).context().authentication().addUser("test" + i, "passwd_test" + i);

        final IgniteInternalFuture restartFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
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
                    e.printStackTrace(System.err);
                    fail("Unexpected exception on server restart: " + e.getMessage());
                }
            }
        });

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                String user = "test" + usrCnt.getAndIncrement();

                try {
                    while (!restartFut.isDone()) {
                        AuthorizationContext actx = grid(CLI_NODE).context().authentication()
                            .authenticate(user, "passwd_" + user);

                        assertNotNull(actx);
                    }
                }
                catch (IgniteCheckedException e) {
                    // Skip exception if server down.
                    if (!e.getMessage().contains("Failed to send message (node may have left the grid or "
                        + "TCP connection cannot be established due to firewall issues)")) {
                        e.printStackTrace();
                        fail("Unexpected exception: " + e.getMessage());
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception: " + e.getMessage());
                }
            }
        }, testUsersCnt, "user-op");

        restartFut.get();
    }

    /**
     * @return Future.
     */
    protected IgniteInternalFuture restartCoordinator() {
        return GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
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
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void test1kUsersNodeRestartServer() throws Exception {
        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);

                try {
                    while (usrCnt.get() < 200) {
                        String user = "test" + usrCnt.getAndIncrement();

                        System.out.println("+++ CREATE  " + user);
                        grid(0).context().authentication().addUser(user, "init");
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception on add / remove");
                }
            }
        }, 3, "user-op");

        usrCnt.set(0);

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);

                try {
                    while (usrCnt.get() < 200) {
                        String user = "test" + usrCnt.getAndIncrement();

                        System.out.println("+++ ALTER " + user);

                        grid(0).context().authentication().updateUser(user, "passwd_" + user);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception on add / remove");
                }
            }
        }, 3, "user-op");

        System.out.println("+++ STOP");
        stopGrid(0, true);

        U.sleep(1000);

        System.out.println("+++ START");
        startGrid(0);

        AuthorizationContext actx = grid(0).context().authentication().authenticate("ignite", "ignite");
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentAddUpdateRemoveNodeRestartServer() throws Exception {
        final IgniteInternalFuture restartFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 0; i < RESTARTS; ++i) {
                        stopGrid(1);

                        U.sleep(500);

                        startGrid(1);

                        U.sleep(500);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace(System.err);
                    fail("Unexpected exception on server restart: " + e.getMessage());
                }
            }
        });

        AuthorizationContext.context(actxDflt);

        final AtomicInteger usrCnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);

                String user = "test" + usrCnt.getAndIncrement();

                try {
                    while (!restartFut.isDone()) {
                        grid(CLI_NODE).context().authentication().addUser(user, "init");

                        grid(CLI_NODE).context().authentication().updateUser(user, "passwd_" + user);

                        grid(CLI_NODE).context().authentication().removeUser(user);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected exception on add / remove");
                }
            }
        }, 10, "user-op");

        restartFut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentFailedOperationNodeRestartServer() throws Exception {
        final IgniteInternalFuture restartFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    for (int i = 0; i < RESTARTS; ++i) {
                        stopGrid(1);

                        U.sleep(500);

                        startGrid(1);

                        U.sleep(500);
                    }
                }
                catch (Exception e) {
                    e.printStackTrace(System.err);
                    fail("Unexpected exception on server restart: " + e.getMessage());
                }
            }
        });

        AuthorizationContext.context(actxDflt);

        grid(CLI_NODE).context().authentication().addUser("test", "test");

        GridTestUtils.runMultiThreaded(new Runnable() {
            @Override public void run() {
                AuthorizationContext.context(actxDflt);

                try {
                    while (!restartFut.isDone()) {
                        GridTestUtils.assertThrows(log, new Callable<Object>() {
                            @Override public Object call() throws Exception {
                                grid(CLI_NODE).context().authentication().addUser("test", "test");

                                return null;
                            }
                        }, UserManagementException.class, "User already exists");
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                    fail("Unexpected error on failed operation");
                }
            }
        }, 10, "user-op");

        restartFut.get();
    }
}
