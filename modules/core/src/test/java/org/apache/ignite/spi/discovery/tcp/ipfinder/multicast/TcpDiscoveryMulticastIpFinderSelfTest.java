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

package org.apache.ignite.spi.discovery.tcp.ipfinder.multicast;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * GridTcpDiscoveryMulticastIpFinder test.
 */
public class TcpDiscoveryMulticastIpFinderSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryMulticastIpFinder> {
    /**
     * @throws Exception In case of error.
     */
    public TcpDiscoveryMulticastIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryMulticastIpFinder ipFinder() throws Exception {
        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setMulticastGroup(GridTestUtils.getNextMulticastGroup(getClass()));
        ipFinder.setMulticastPort(GridTestUtils.getNextMulticastPort(getClass()));

        return ipFinder;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    @Test
    public void testExchange() throws Exception {
        String locAddr = null;

        TcpDiscoveryMulticastIpFinder ipFinder1 = null;
        TcpDiscoveryMulticastIpFinder ipFinder2 = null;
        TcpDiscoveryMulticastIpFinder ipFinder3 = null;

        try {
            ipFinder1 = ipFinder();
            ipFinder1.setResponseWaitTime(1000);
            ipFinder1.setAddressRequestAttempts(5);

            ipFinder2 = new TcpDiscoveryMulticastIpFinder();

            ipFinder2.setResponseWaitTime(1000);
            ipFinder2.setAddressRequestAttempts(5);
            ipFinder2.setMulticastGroup(ipFinder1.getMulticastGroup());
            ipFinder2.setMulticastPort(ipFinder1.getMulticastPort());

            ipFinder3 = new TcpDiscoveryMulticastIpFinder();

            ipFinder3.setResponseWaitTime(1000);
            ipFinder3.setAddressRequestAttempts(5);
            ipFinder3.setMulticastGroup(ipFinder1.getMulticastGroup());
            ipFinder3.setMulticastPort(ipFinder1.getMulticastPort());

            injectLogger(ipFinder1);
            injectLogger(ipFinder2);
            injectLogger(ipFinder3);

            ipFinder1.setLocalAddress(locAddr);
            ipFinder2.setLocalAddress(locAddr);
            ipFinder3.setLocalAddress(locAddr);

            ipFinder1.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host1", 1001)));

            Collection<InetSocketAddress> addrs1 = ipFinder1.getRegisteredAddresses();

            ipFinder2.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host2", 1002)));

            Collection<InetSocketAddress> addrs2 = ipFinder2.getRegisteredAddresses();

            ipFinder3.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host3", 1003)));

            Collection<InetSocketAddress> addrs3 = ipFinder3.getRegisteredAddresses();

            info("Addrs1: " + addrs1);
            info("Addrs2: " + addrs2);
            info("Addrs2: " + addrs3);

            assertEquals(1, addrs1.size());
            assertEquals(2, addrs2.size());
            assertTrue("Unexpected number of addresses: " + addrs3, addrs3.size() == 2 || addrs3.size() == 3);

            checkRequestAddresses(ipFinder1, 3);
            checkRequestAddresses(ipFinder2, 3);
            checkRequestAddresses(ipFinder3, 3);
        }
        finally {
            if (ipFinder1 != null)
                ipFinder1.close();

            if (ipFinder2 != null)
                ipFinder2.close();

            if (ipFinder3 != null)
                ipFinder3.close();
        }
    }

    /**
     * @param ipFinder IP finder.
     * @param exp Expected number of addresses.
     */
    private void checkRequestAddresses(TcpDiscoveryMulticastIpFinder ipFinder, int exp) {
        for (int i = 0; i < 10; i++) {
            if (ipFinder.getRegisteredAddresses().size() == exp)
                return;
        }

        assertEquals(exp, ipFinder.getRegisteredAddresses().size());
    }
}
