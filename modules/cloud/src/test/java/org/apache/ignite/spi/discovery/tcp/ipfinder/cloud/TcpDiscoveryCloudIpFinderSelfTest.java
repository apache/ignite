/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.cloud;

import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.Collection;

import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteCloudTestSuite;
import org.apache.ignite.testsuites.IgniteIgnore;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TcpDiscoveryCloudIpFinder test.
 */
@RunWith(JUnit4.class)
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryCloudIpFinder ipFinder() {
        // No-op.
        return null;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() {
        // No-op
    }

    /**
     * Tests AWS.
     *
     * @throws Exception If any error occurs.
     */
    @IgniteIgnore("https://issues.apache.org/jira/browse/IGNITE-845")
    @Test
    public void testAmazonWebServices() throws Exception {
        testCloudProvider("aws-ec2");
    }

    /**
     * Tests GCE.
     *
     * @throws Exception If any error occurs.
     */
    @IgniteIgnore("https://issues.apache.org/jira/browse/IGNITE-1585")
    @Test
    public void testGoogleComputeEngine() throws Exception {
        testCloudProvider("google-compute-engine");
    }

    /**
     * Tests Rackspace.
     *
     * @throws Exception If any error occurs.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9444")
    @Test
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
