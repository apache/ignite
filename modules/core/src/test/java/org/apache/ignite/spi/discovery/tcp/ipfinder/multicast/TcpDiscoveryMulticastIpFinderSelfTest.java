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
    @SuppressWarnings({"TooBroadScope", "BusyWait"})
    public void testExchange() throws Exception {
        String locAddr = null;

        TcpDiscoveryMulticastIpFinder ipFinder1 = null;
        TcpDiscoveryMulticastIpFinder ipFinder2 = null;
        TcpDiscoveryMulticastIpFinder ipFinder3 = null;

        try {
            ipFinder1 = ipFinder();

            ipFinder2 = new TcpDiscoveryMulticastIpFinder();

            ipFinder2.setMulticastGroup(ipFinder1.getMulticastGroup());
            ipFinder2.setMulticastPort(ipFinder1.getMulticastPort());

            ipFinder3 = new TcpDiscoveryMulticastIpFinder();

            ipFinder3.setMulticastGroup(ipFinder1.getMulticastGroup());
            ipFinder3.setMulticastPort(ipFinder1.getMulticastPort());

            injectLogger(ipFinder1);
            injectLogger(ipFinder2);
            injectLogger(ipFinder3);

            ipFinder1.setLocalAddress(locAddr);
            ipFinder2.setLocalAddress(locAddr);
            ipFinder3.setLocalAddress(locAddr);

            ipFinder1.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host1", 1001)));
            ipFinder2.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host2", 1002)));
            ipFinder3.initializeLocalAddresses(Collections.singleton(new InetSocketAddress("host3", 1003)));

            for (int i = 0; i < 5; i++) {
                Collection<InetSocketAddress> addrs1 = ipFinder1.getRegisteredAddresses();
                Collection<InetSocketAddress> addrs2 = ipFinder2.getRegisteredAddresses();
                Collection<InetSocketAddress> addrs3 = ipFinder3.getRegisteredAddresses();

                if (addrs1.size() != 1 || addrs2.size() != 2 || addrs3.size() != 3) {
                    info("Addrs1: " + addrs1);
                    info("Addrs2: " + addrs2);
                    info("Addrs2: " + addrs3);

                    Thread.sleep(1000);
                }
                else
                    break;
            }

            assertEquals(1, ipFinder1.getRegisteredAddresses().size());
            assertEquals(2, ipFinder2.getRegisteredAddresses().size());
            assertEquals(3, ipFinder3.getRegisteredAddresses().size());
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
}