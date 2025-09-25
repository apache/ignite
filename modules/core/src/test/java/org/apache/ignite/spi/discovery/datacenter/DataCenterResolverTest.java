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

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests for {@link DataCenterResolver} feature.
 */
public class DataCenterResolverTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_ID = "DC0";

    /** */
    private static final DataCenterResolver NULL_DC_RESOLVER = new DataCenterResolver() {
        /** {@inheritDoc} */
        @Override public String resolveDataCenterId() {
            return null;
        }
    };

    /** */
    private static final DataCenterResolver EMPTY_DC_RESOLVER = new DataCenterResolver() {
        /** {@inheritDoc} */
        @Override public String resolveDataCenterId() {
            return "";
        }
    };

    /** */
    private DataCenterResolver dcResolver;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (dcResolver != null)
            cfg.setDataCenterResolver(dcResolver);

        // To speed up tests involving nodes shutting down.
        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
        discoSpi.setNetworkTimeout(500);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        dcResolver = null;
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
        dcResolver = new SystemPropertyDataCenterResolver();

        IgniteEx testGrid = startGrid();

        Object dcId = testGrid.localNode().attribute(IgniteNodeAttributes.ATTR_DATA_CENTER_ID);

        assertEquals(DC_ID, dcId);
    }

    /**
     * Verifies that node with configured DataCenterResolver fails to start if the resolver returns {@code null} or empty DC_ID.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyDcIdIsNotAllowed() throws Exception {
        dcResolver = NULL_DC_RESOLVER;

        assertThrows(log, () -> startGrid(), IgniteCheckedException.class, "Data center ID should not be empty");

        dcResolver = EMPTY_DC_RESOLVER;

        assertThrows(log, () -> startGrid(), IgniteCheckedException.class, "Data center ID should not be empty");
    }

    /**
     * Verifies that cluster with configured DataCenterResolver rejects server nodes without DataCenterResolver and vise versa.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IgniteSystemProperties.IGNITE_DATA_CENTER_ID, value = DC_ID)
    public void testServerWithWrongDcIdConfigIsProhibitedToJoin() throws Exception {
        dcResolver = new SystemPropertyDataCenterResolver();
        startGrid(0);

        dcResolver = null;

        try {
            startGrid(1);
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
            assertTrue(errMsg.contains("Remote node declares DataCenter ID but local node doesn't"));
        }

        stopAllGrids();

        dcResolver = null;
        startGrid(0);

        dcResolver = new SystemPropertyDataCenterResolver();

        try {
            startGrid(1);
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
            assertTrue(errMsg.contains("Local node declares DataCenter ID but remote node doesn't"));
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
        dcResolver = new SystemPropertyDataCenterResolver();
        startGrid(0);

        try {
            startClientGrid(1);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }

        dcResolver = null;

        try {
            startClientGrid(2);
        }
        catch (IgniteCheckedException e) {
            assertFalse("Unexpected exception was thrown: " + e, true);
        }
    }
}
