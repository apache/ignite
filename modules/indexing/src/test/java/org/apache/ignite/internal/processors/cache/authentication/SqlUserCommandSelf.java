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

package org.apache.ignite.internal.processors.cache.authentication;

import java.util.Base64;
import java.util.Random;
import org.apache.ignite.configuration.AuthenticationConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.authentication.AuthorizationContext;
import org.apache.ignite.internal.processors.authentication.User;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for leaks JdbcConnection on SqlFieldsQuery execute.
 */
public class SqlUserCommandSelf extends GridCommonAbstractTest {
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
    public void testDefaultUser() throws Exception {
        for (int i = 0; i < NODES_COUNT; ++i) {
            AuthorizationContext actx = grid(0).context().authentication().authenticate("ignite", "ignite");

            assertNotNull(actx);
            assertEquals("ignite", actx.userName());
        }
    }
}
