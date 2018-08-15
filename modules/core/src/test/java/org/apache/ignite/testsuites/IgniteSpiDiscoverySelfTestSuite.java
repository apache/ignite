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

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.IgniteDiscoveryMassiveNodeFailTest;
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.AuthenticationRestartTest;
import org.apache.ignite.spi.discovery.FilterDataForClientNodeDiscoveryTest;
import org.apache.ignite.spi.discovery.IgniteDiscoveryCacheReuseSelfTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryUnmarshalVulnerabilityTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientSuspensionSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryConcurrentStartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMultiThreadedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeAttributesUpdateOnReconnectTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConfigConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryRestartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySegmentationPolicyTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySnapshotHistoryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiConfigSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiMBeanTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiRandomStartStopTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiReconnectDelayTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiStartStopSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiWildcardSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSecuredUnsecuredTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedUntrustedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryWithWrongServerTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.TcpDiscoveryJdbcIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.TcpDiscoverySharedFsIpFinderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinderSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Test suite for all discovery spi implementations.
 */
public class IgniteSpiDiscoverySelfTestSuite extends TestSuite {
    /**
     * @return Discovery SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteSpiDiscoverySelfTestSuite.class));

        TestSuite suite = new TestSuite("Ignite Discovery SPI Test Suite");

        // Tcp.
        suite.addTest(new TestSuite(TcpDiscoveryVmIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySharedFsIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryJdbcIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryMulticastIpFinderSelfTest.class));

        suite.addTest(new TestSuite(TcpDiscoverySelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiSelfTest.class));
        //suite.addTest(new TestSuite(TcpDiscoverySpiRandomStartStopTest.class));
        //suite.addTest(new TestSuite(TcpDiscoverySpiSslSelfTest.class));
        //suite.addTest(new TestSuite(TcpDiscoverySpiWildcardSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiFailureTimeoutSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiMBeanTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiConfigSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySnapshotHistoryTest.class));

        suite.addTest(new TestSuite(GridTcpSpiForwardingSelfTest.class));

        suite.addTest(new TestSuite(TcpClientDiscoverySpiSelfTest.class));
        suite.addTest(new TestSuite(TcpClientDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new TestSuite(TcpClientDiscoverySpiMulticastTest.class));
        suite.addTest(new TestSuite(TcpClientDiscoverySpiFailureTimeoutSelfTest.class));

        suite.addTest(new TestSuite(TcpDiscoveryNodeConsistentIdSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryNodeConfigConsistentIdSelfTest.class));

        suite.addTest(new TestSuite(TcpDiscoveryRestartTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryMultiThreadedTest.class));
        //suite.addTest(new TestSuite(TcpDiscoveryConcurrentStartTest.class));

        suite.addTest(new TestSuite(TcpDiscoverySegmentationPolicyTest.class));

        suite.addTest(new TestSuite(TcpDiscoveryNodeAttributesUpdateOnReconnectTest.class));
        suite.addTest(new TestSuite(AuthenticationRestartTest.class));

        suite.addTest(new TestSuite(TcpDiscoveryWithWrongServerTest.class));

        suite.addTest(new TestSuite(TcpDiscoverySpiReconnectDelayTest.class));

        suite.addTest(new TestSuite(IgniteDiscoveryMassiveNodeFailTest.class));

        // Client connect.
        suite.addTest(new TestSuite(IgniteClientConnectTest.class));
        suite.addTest(new TestSuite(IgniteClientReconnectMassiveShutdownTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryClientSuspensionSelfTest.class));

        // SSL.
        suite.addTest(new TestSuite(TcpDiscoverySslSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySslTrustedSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySslSecuredUnsecuredTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySslTrustedUntrustedTest.class));

        // Disco cache reuse.
        suite.addTest(new TestSuite(IgniteDiscoveryCacheReuseSelfTest.class));

        suite.addTest(new TestSuite(DiscoveryUnmarshalVulnerabilityTest.class));

        suite.addTest(new TestSuite(FilterDataForClientNodeDiscoveryTest.class));

        suite.addTest(new TestSuite(TcpDiscoveryPendingMessageDeliveryTest.class));

        return suite;
    }
}
