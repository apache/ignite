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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
     * Verifies that attempt to start a node with DC ID resolved to null or empty string ends up with an exception.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyDCIdIsNotAllowed() throws Exception {
        dcResolver = NULL_DC_RESOLVER;

        GridTestUtils.assertThrows(log, () -> startGrid(), IgniteCheckedException.class, "Data center ID should not be empty");

        dcResolver = EMPTY_DC_RESOLVER;

        GridTestUtils.assertThrows(log, () -> startGrid(), IgniteCheckedException.class, "Data center ID should not be empty");
    }
}
