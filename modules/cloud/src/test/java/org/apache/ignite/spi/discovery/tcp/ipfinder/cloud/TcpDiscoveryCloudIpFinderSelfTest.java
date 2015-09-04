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

import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteCloudTestSuite;

/**
 * TcpDiscoveryCloudIpFinder test.
 */
public class TcpDiscoveryCloudIpFinderSelfTest extends
    TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryCloudIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryCloudIpFinderSelfTest() throws Exception {
        // No-op.
    }

    @Override protected void beforeTest() throws Exception {
        // No-op.
    }

    /* {@inheritDoc} */
    @Override protected TcpDiscoveryCloudIpFinder ipFinder() throws Exception {
        // No-op.
        return null;
    }

    /* {@inheritDoc} */
    @Override public void testIpFinder() throws Exception {
        // No-op
    }

    /**
     * Tests AWS.
     *
     * @throws Exception If any error occurs.
     */
    public void testAmazonWebServices() throws Exception {
        testCloudProvider("aws-ec2");
    }

    /**
     * Tests GCE.
     *
     * @throws Exception If any error occurs.
     */
    public void testGoogleComputeEngine() throws Exception {
        testCloudProvider("google-compute-engine");
    }

    /**
     * Tests Rackspace.
     *
     * @throws Exception If any error occurs.
     */
    public void testRackspace() throws Exception {
        testCloudProvider("rackspace-cloudservers-us");
    }

    /**
     * Tests a given provider.
     *
     * @param provider Provider name.
     * @throws Exception If any error occurs.
     */
    private void testCloudProvider(String provider) throws Exception {
        info("Testing provider: " + provider);

        TcpDiscoveryCloudIpFinder ipFinder = new TcpDiscoveryCloudIpFinder();

        injectLogger(ipFinder);

        ipFinder.setProvider(provider);
        ipFinder.setIdentity(IgniteCloudTestSuite.getAccessKey(provider));
        ipFinder.setRegions(IgniteCloudTestSuite.getRegions(provider));
        ipFinder.setZones(IgniteCloudTestSuite.getZones(provider));

        if (provider.equals("google-compute-engine"))
            ipFinder.setCredentialPath(IgniteCloudTestSuite.getSecretKey(provider));
        else
            ipFinder.setCredential(IgniteCloudTestSuite.getSecretKey(provider));

        Collection<InetSocketAddress> addresses = ipFinder.getRegisteredAddresses();

        for (InetSocketAddress addr : addresses)
            info("Registered instance: " + addr.getAddress().getHostAddress() + ":" + addr.getPort());

        ipFinder.unregisterAddresses(addresses);

        assert addresses.size() == ipFinder.getRegisteredAddresses().size();

        ipFinder.registerAddresses(ImmutableList.of(
            new InetSocketAddress("192.168.0.1", TcpDiscoverySpi.DFLT_PORT)));

        assert addresses.size() == ipFinder.getRegisteredAddresses().size();
    }
}