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

package org.apache.ignite.internal.processors.cluster;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.AddressResolver;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 * Address Resolver test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridAddressResolverSelfTest extends GridCommonAbstractTest {
    /** */
    private final InetSocketAddress addr0 = new InetSocketAddress("test0.com", 5000);

    /** */
    private final InetSocketAddress addr1 = new InetSocketAddress("test1.com", 5000);

    /** Ip finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        cfg.setAddressResolver(new AddressResolver() {
            @Override public Collection<InetSocketAddress> getExternalAddresses(
                InetSocketAddress addr) throws IgniteCheckedException {
                Set<InetSocketAddress> set = new HashSet<>();

                set.add(addr);
                set.add(igniteInstanceName.contains("0") ? addr0 : addr1);

                return set;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrid(0);

        assertFalse(IP_FINDER.getRegisteredAddresses().contains(addr1));

        startGrid(1);

        assertTrue(IP_FINDER.getRegisteredAddresses().contains(addr0));
        assertTrue(IP_FINDER.getRegisteredAddresses().contains(addr1));

        stopGrid(0, true);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return !IP_FINDER.getRegisteredAddresses().contains(addr0);
            }
        }, 70000));
    }
}
