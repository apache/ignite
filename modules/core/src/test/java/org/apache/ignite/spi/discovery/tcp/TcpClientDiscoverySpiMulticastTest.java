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

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class TcpClientDiscoverySpiMulticastTest extends GridCommonAbstractTest {
    /** */
    private boolean forceSrv;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(new TcpDiscoveryMulticastIpFinder());

        if (getTestGridName(1).equals(gridName)) {
            cfg.setClientMode(true);

            spi.setForceServerMode(forceSrv);
        }

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }
    /**
     * @throws Exception If failed.
     */
    public void testJoinWithMulticast() throws Exception {
        joinWithMulticast();
    }

    /**
     * @throws Exception If failed.
     */
    public void testJoinWithMulticastForceServer() throws Exception {
        forceSrv = true;

        joinWithMulticast();
    }

    /**
     * @throws Exception If failed.
     */
    private void joinWithMulticast() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertSpi(ignite0, false);

        Ignite ignite1 = startGrid(1);

        assertSpi(ignite1, !forceSrv);

        assertTrue(ignite1.configuration().isClientMode());

        assertEquals(2, ignite0.cluster().nodes().size());
        assertEquals(2, ignite1.cluster().nodes().size());

        Ignite ignite2 = startGrid(2);

        assertSpi(ignite2, false);

        assertEquals(3, ignite0.cluster().nodes().size());
        assertEquals(3, ignite1.cluster().nodes().size());
        assertEquals(3, ignite2.cluster().nodes().size());
    }

    /**
     * @param ignite Ignite.
     * @param client Expected client mode flag.
     */
    private void assertSpi(Ignite ignite, boolean client) {
        DiscoverySpi spi = ignite.configuration().getDiscoverySpi();

        assertSame(TcpDiscoverySpi.class, spi.getClass());

        TcpDiscoverySpi spi0 = (TcpDiscoverySpi)spi;

        assertSame(TcpDiscoveryMulticastIpFinder.class, spi0.getIpFinder().getClass());

        assertEquals(client, spi0.isClientMode());

        Collection<Object> addrSnds = GridTestUtils.getFieldValue(spi0.getIpFinder(), "addrSnds");

        assertNotNull(addrSnds);

        if (client)
            assertTrue(addrSnds.isEmpty()); // Check client does not send its address.
        else
            assertFalse(addrSnds.isEmpty());
    }
}