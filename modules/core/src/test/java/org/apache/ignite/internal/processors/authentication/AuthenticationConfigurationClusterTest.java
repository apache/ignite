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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for disabled {@link IgniteAuthenticationProcessor}.
 */
public class AuthenticationConfigurationClusterTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setAuthenticationEnabled(authEnabled);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)));

        return cfg;
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
    public void testServerNodeJoinDisabled() throws Exception {
        startGrid(configuration(0, true, false));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(1, false, false));

                    return null;
                }
            },
            IgniteCheckedException.class,
            "User authentication configuration is different on local server node and coordinator");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeJoinDisabled() throws Exception {
        startGrid(configuration(0, true, false));

        startGrid(configuration(1, false, true));

        grid(0).cluster().active(true);

        AuthorizationContext actx = grid(1).context().authentication().authenticate("ignite", "ignite");

        assertNotNull(actx);

        assertEquals("ignite", actx.userName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNodeJoinEnabled() throws Exception {
        startGrid(configuration(0, false, false));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(1, true, false));

                    return null;
                }
            },
            IgniteCheckedException.class,
            "User authentication configuration is different on local server node and coordinator");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNodeJoinEnabled() throws Exception {
        startGrid(configuration(0, false, false));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    startGrid(configuration(1, true, true));

                    return null;
                }
            },
            IgniteCheckedException.class,
            "User authentication is disabled on cluster");
    }

    /**
     * @throws Exception If failed.
     */
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
}
