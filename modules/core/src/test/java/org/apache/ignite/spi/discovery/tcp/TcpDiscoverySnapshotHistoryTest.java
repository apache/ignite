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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiHistorySupport;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.DFLT_TOP_HISTORY_SIZE;

/**
 * Tests for topology snapshots history.
 */
public class TcpDiscoverySnapshotHistoryTest extends GridCommonAbstractTest {
    /** */
    public TcpDiscoverySnapshotHistoryTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());
        cfg.setCacheConfiguration();
        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testHistorySupported() throws Exception {
        try {
            final Ignite g = startGrid();

            DiscoverySpi spi = g.configuration().getDiscoverySpi();

            DiscoverySpiHistorySupport ann = U.getAnnotation(spi.getClass(), DiscoverySpiHistorySupport.class);

            assertNotNull("Spi does not have annotation for history support", ann);

            assertTrue("History support is disabled for current spi", ann.value());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testSettingNewTopologyHistorySize() throws Exception {
        try {
            final Ignite g = startGrid();

            TcpDiscoverySpi spi = (TcpDiscoverySpi)g.configuration().getDiscoverySpi();

            assertEquals(DFLT_TOP_HISTORY_SIZE, spi.getTopHistorySize());

            spi.setTopHistorySize(DFLT_TOP_HISTORY_SIZE + 1);

            assertEquals(DFLT_TOP_HISTORY_SIZE + 1, spi.getTopHistorySize());

            spi.setTopHistorySize(1);

            assertEquals(DFLT_TOP_HISTORY_SIZE + 1, spi.getTopHistorySize());
        }
        finally {
            stopGrid();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testNodeAdded() throws Exception {
        try {
            // Add grid #1
            final Ignite g1 = startGrid(1);

            assertTopVer(1, g1);

            assertEquals(1, g1.cluster().topologyVersion());

            // Add grid # 2
            final Ignite g2 = startGrid(2);

            assertTopVer(2, g1, g2);

            for (int i = 1; i <= 2; i++)
                assertEquals(i, g2.cluster().topology(i).size());

            // Add grid # 3
            final Ignite g3 = startGrid(3);

            assertTopVer(3, g1, g2, g3);

            for (int i = 1; i <= 3; i++)
                assertEquals(i, g3.cluster().topology(i).size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If any error occurs.
     */
    public void testNodeAddedAndRemoved() throws Exception {
        try {
            // Add grid #1
            final Ignite g1 = startGrid(1);

            assertTopVer(1, g1);

            assertEquals(1, g1.cluster().topologyVersion());

            // Add grid #2
            final Ignite g2 = startGrid(2);

            assertTopVer(2, g1, g2);

            for (int i = 1; i <= 2; i++)
                assertEquals(i, g2.cluster().topology(i).size());

            // Add grid #3
            final Ignite g3 = startGrid(3);

            assertTopVer(3, g1, g2, g3);

            for (int i = 1; i <= 3; i++)
                assertEquals(i, g3.cluster().topology(i).size());

            // Stop grid #3
            stopGrid(g3.name());

            assertTopVer(4, g1, g2);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Check if specified grid instances have unexpected topology version.
     *
     * @param expTopVer Expected topology version.
     * @param ignites Grid instances for checking topology version.
     */
    private static void assertTopVer(long expTopVer, Ignite... ignites) {
        for (Ignite g : ignites)
            assertEquals("Grid has wrong topology version.", expTopVer, g.cluster().topologyVersion());
    }
}