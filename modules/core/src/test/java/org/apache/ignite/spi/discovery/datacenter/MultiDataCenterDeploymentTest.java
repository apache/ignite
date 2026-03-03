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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test scenarios for Data Center ID configuration variations.
 */
public class MultiDataCenterDeploymentTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID_0 = "DC0";

    /** */
    private static final String DC_ID_1 = "DC1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // To speed up tests involving nodes shutting down.
        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setNetworkTimeout(500);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);
    }

    /**
     * Verifies that ClusterNode picks up Data Center ID from the corresponding system property.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DATA_CENTER_ID, value = DC_ID_0)
    public void testAttributeSetLocallyFromSystemProperty() throws Exception {
        IgniteEx testGrid = startGrid();

        String dcId = testGrid.localNode().dataCenterId();

        assertNotNull("Data Center ID of the node should not be null", dcId);
        assertEquals(DC_ID_0, dcId);

        String dcIdFromAttr = testGrid.localNode().attribute(IgniteNodeAttributes.ATTR_DATA_CENTER_ID);
        assertNotNull("Data Center ID of the node should not be null", dcIdFromAttr);
        assertEquals(DC_ID_0, dcIdFromAttr);
    }

    /**
     * Verifies that cluster with configured Data Center ID rejects server nodes without one and vise versa.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMixedDcConfigurationIsProhibited() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

        startGrid(0);

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

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
            assertTrue(errMsg.contains("Data Center ID is specified for remote node but not for local node"));
        }

        stopAllGrids();

        startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);

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
            assertTrue(errMsg.contains("Data Center ID is specified for local node but not for remote node"));
        }
    }

    /**
     *  Verifies that servers from different Data Centers can form a single cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServersFromDifferentDcsFormACluster() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        IgniteEx srv0 = startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);
        IgniteEx srv1 = startGrid(1);

        waitForTopology(2);

        assertEquals(srv0.localNode().dataCenterId(), DC_ID_0);
        assertEquals(srv1.localNode().dataCenterId(), DC_ID_1);
    }

    /**
     * Verifies that client nodes with or without Data Center ID specified are allowed to join a cluster with a configured Data Center ID.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientWithoutDcIdIsAllowedToJoin() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_0);
        IgniteEx srv0 = startGrid(0);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_ID_1);

        IgniteEx client0 = null;

        try {
            client0 = startClientGrid(1);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }

        System.clearProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID);

        IgniteEx client1 = null;

        try {
            client1 = startClientGrid(2);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }

        assertEquals(srv0.localNode().dataCenterId(), DC_ID_0);

        assertNotNull(client0);
        assertEquals(client0.localNode().dataCenterId(), DC_ID_1);

        assertNotNull(client1);
        assertNull(client1.localNode().dataCenterId(), null);
    }
}
