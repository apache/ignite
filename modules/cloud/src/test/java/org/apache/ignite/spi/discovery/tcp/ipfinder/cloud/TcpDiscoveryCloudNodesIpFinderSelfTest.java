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

package org.apache.ignite.spi.discovery.tcp.ipfinder.cloud;

import com.google.common.collect.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.testsuites.*;

import java.net.*;
import java.util.*;

/**
 * TcpDiscoveryCloudNodesIpFinder test.
 */
public class TcpDiscoveryCloudNodesIpFinderSelfTest extends
    TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryCloudNodesIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryCloudNodesIpFinderSelfTest() throws Exception {
        // No-op.
    }

    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /* {@inheritDoc} */
    @Override protected TcpDiscoveryCloudNodesIpFinder ipFinder() throws Exception {
        // No-op.
        return null;
    }

    /* {@inheritDoc} */
    @Override public void testIpFinder() throws Exception {
        TcpDiscoveryCloudNodesIpFinder ipFinder;
        String[] providers = {"google-compute-engine", "aws-ec2", "rackspace-cloudservers-us"};

        for (String provider : providers) {
            info("Testing provider: " + provider);

            ipFinder = new TcpDiscoveryCloudNodesIpFinder();
            injectLogger(ipFinder);

            ipFinder.setProvider(provider);
            ipFinder.setIdentity(IgniteCloudTestSuite.getAccessKey(provider));
            ipFinder.setRegions(IgniteCloudTestSuite.getRegions(provider));
            ipFinder.setZones(IgniteCloudTestSuite.getZones(provider));

            if (provider.equals("google-compute-engine"))
                ipFinder.setCredentialPath(IgniteCloudTestSuite.getSecretKey(provider));
            else {
                ipFinder.setCredential(IgniteCloudTestSuite.getSecretKey(provider));
            }

            ipFinder.setDiscoveryPort(TcpDiscoverySpi.DFLT_PORT);

            Collection<InetSocketAddress> addresses = ipFinder.getRegisteredAddresses();

            assert addresses.size() > 0;

            for (InetSocketAddress addr: addresses)
                info("Registered instance: " + addr.getAddress().getHostAddress() + ":" + addr.getPort());

            ipFinder.unregisterAddresses(addresses);

            assert addresses.size() == ipFinder.getRegisteredAddresses().size();

            ipFinder.registerAddresses(ImmutableList.of(
                new InetSocketAddress("192.168.0.1", TcpDiscoverySpi.DFLT_PORT)));

            assert addresses.size() == ipFinder.getRegisteredAddresses().size();
        }
    }
}
