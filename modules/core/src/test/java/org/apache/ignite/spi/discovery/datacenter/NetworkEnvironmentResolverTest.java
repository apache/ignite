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

package org.apache.ignite.spi.discovery.datacenter;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.NetworkEnvironment;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.NetworkEnvironmentImpl;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;


/**
 * Tests for {@link NetworkEnvironmentResolver} feature.
 */
public class NetworkEnvironmentResolverTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID = "DC0";

    /** */
    private static final NetworkEnvironmentResolver NULL_DC_NET_ENV_RESOLVER = new NetworkEnvironmentResolver() {
        /** {@inheritDoc} */
        @Override public NetworkEnvironment resolveNetworkEnvironment() {
            return new NetworkEnvironmentImpl(null);
        }
    };

    /** */
    private static final NetworkEnvironmentResolver EMPTY_DC_NET_ENV_RESOLVER = new NetworkEnvironmentResolver() {
        /** {@inheritDoc} */
        @Override public NetworkEnvironment resolveNetworkEnvironment() {
            return new NetworkEnvironmentImpl("");
        }
    };

    /** */
    private NetworkEnvironmentResolver netEnvResolver;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (netEnvResolver != null)
            cfg.setNetworkEnvironmentResolver(netEnvResolver);

        // To speed up tests involving nodes shutting down.
        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setNetworkTimeout(500);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        netEnvResolver = null;
    }

    /**
     * Verifies that node's attribute is not set if no resolver is configured.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAttributeNotSetIfNoResolverConfigured() throws Exception {
        IgniteEx testGrid = startGrid();

        Object dcId = testGrid.localNode().attribute(IgniteNodeAttributes.ATTR_DATA_CENTER_ID);

        assertNull(dcId);
    }

    /**
     * Verifies that node's attribute for DC ID is set from system property.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DATA_CENTER_ID, value = DC_ID)
    public void testAttributeSetLocallyFromSysProp() throws Exception {
        netEnvResolver = new SystemPropertyNetworkEnvironmentResolver();

        IgniteEx testGrid = startGrid();

        NetworkEnvironment netEnv = testGrid.localNode().networkEnvironment();

        assertNotNull("NetworkEnvironment of the node should not be null", netEnv);

        assertEquals(DC_ID, netEnv.dataCenterId());
    }

    /**
     * Verifies that node with configured DataCenterResolver fails to start if the resolver returns {@code null} or empty DC_ID.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyDcIdIsNotAllowed() throws Exception {
        netEnvResolver = NULL_DC_NET_ENV_RESOLVER;

        try {
            startGrid();
        } catch (Throwable t) {
            IgniteSpiException cause = X.cause(t, IgniteSpiException.class);
            assertNotNull(cause);
            assertNotNull(cause.getMessage());
            assertTrue(cause.getMessage().contains("Data center ID should not be empty"));
        }

        netEnvResolver = EMPTY_DC_NET_ENV_RESOLVER;

        try {
            startGrid();
        } catch (Throwable t) {
            IgniteSpiException cause = X.cause(t, IgniteSpiException.class);
            assertNotNull(cause);
            assertNotNull(cause.getMessage());
            assertTrue(cause.getMessage().contains("Data center ID should not be empty"));
        }
    }

    /**
     * Verifies that cluster with configured DataCenterResolver rejects server nodes without DataCenterResolver and vise versa.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DATA_CENTER_ID, value = DC_ID)
    public void testServerWithWrongDcIdConfigIsProhibitedToJoin() throws Exception {
        netEnvResolver = new SystemPropertyNetworkEnvironmentResolver();
        startGrid(0);

        netEnvResolver = null;

        try {
            startGrid(1);

            assertFalse("Expected exception hasn't been thrown", true);
        }
        catch (IgniteCheckedException e) {
            Throwable cause = e.getCause();
            assertNotNull(cause);
            assertTrue(cause instanceof IgniteCheckedException);

            cause = cause.getCause();
            assertNotNull(cause);
            assertTrue(cause instanceof IgniteSpiException);

            String errMsg = cause.getMessage();
            assertNotNull(errMsg);
            assertTrue(errMsg.contains("Remote node has NetworkEnvironment configuration but local node doesn't"));
        }

        stopAllGrids();

        netEnvResolver = null;
        startGrid(0);

        netEnvResolver = new SystemPropertyNetworkEnvironmentResolver();

        try {
            startGrid(1);

            assertFalse("Expected exception hasn't been thrown", true);
        }
        catch (IgniteCheckedException e) {
            Throwable cause = e.getCause();
            assertNotNull(cause);
            assertTrue(cause instanceof IgniteCheckedException);

            cause = cause.getCause();
            assertNotNull(cause);
            assertTrue(cause instanceof IgniteSpiException);

            String errMsg = cause.getMessage();
            assertNotNull(errMsg);
            assertTrue(errMsg.contains("Local node has NetworkEnvironment configuration but remote node doesn't"));
        }
    }

    /**
     * Verifies that clients with or without DataCenterResolver are allowed to join a cluster with a configured one.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DATA_CENTER_ID, value = DC_ID)
    public void testClientWithoutDcIdIsAllowedToJoin() throws Exception {
        netEnvResolver = new SystemPropertyNetworkEnvironmentResolver();
        startGrid(0);

        try {
            startClientGrid(1);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }

        netEnvResolver = null;

        try {
            startClientGrid(2);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }
    }
}
