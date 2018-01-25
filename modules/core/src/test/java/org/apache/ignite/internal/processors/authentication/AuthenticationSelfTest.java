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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link IgniteCompute#affinityCall(String, Object, IgniteCallable)} and
 * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)}.
 */
public class AuthenticationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Client node. */
    private static final int CLI_NODE = 2;

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
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddUserOnServer() throws Exception {
        startGrids(3);

        grid(0).context().authentication().addUser("test", "test");

        User u = grid(0).context().authentication().authenticate("test", "test");

        assertNotNull(u);
        assertEquals("test", u.name());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddUserOnClient() throws Exception {
        startGrids(3);

        checkAddRemoveUser(grid(0), grid(0));
        checkAddRemoveUser(grid(0), grid(1));
        checkAddRemoveUser(grid(1), grid(0));
        checkAddRemoveUser(grid(1), grid(1));
        checkAddRemoveUser(grid(0), grid(CLI_NODE));
        checkAddRemoveUser(grid(CLI_NODE), grid(0));
        checkAddRemoveUser(grid(1), grid(CLI_NODE));
        checkAddRemoveUser(grid(CLI_NODE), grid(1));
        checkAddRemoveUser(grid(CLI_NODE), grid(CLI_NODE));
    }

    /**
     * @param createNode Node to execute create operation.
     * @param authNode Node to execute authentication.
     * @throws Exception On error.
     */
    private void checkAddRemoveUser(IgniteEx createNode, IgniteEx authNode) throws Exception {
        createNode.context().authentication().addUser("test", "test");

        User u = authNode.context().authentication().authenticate("test", "test");

        assertNotNull(u);
        assertEquals("test", u.name());

        createNode.context().authentication().removeUser("test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidUserOnClient() throws Exception {
        startGrids(3);

        grid(CLI_NODE).context().authentication().addUser("test", "test");

        User u = grid(CLI_NODE).context().authentication().authenticate("test", "test");

        assertNotNull(u);
        assertEquals("test", u.name());
    }
}
