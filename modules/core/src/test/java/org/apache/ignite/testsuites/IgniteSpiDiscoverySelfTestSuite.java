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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.IgniteDiscoveryMassiveNodeFailTest;
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.AuthenticationRestartTest;
import org.apache.ignite.spi.discovery.FilterDataForClientNodeDiscoveryTest;
import org.apache.ignite.spi.discovery.IgniteDiscoveryCacheReuseSelfTest;
import org.apache.ignite.spi.discovery.LongClientConnectToClusterTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryUnmarshalVulnerabilityTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiCoordinatorChangeTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryUnresolvedHostTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientSuspensionSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderCleanerTest;
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
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiReconnectDelayTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiStartStopSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslParametersTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Test suite for all discovery spi implementations.
 */
@RunWith(AllTests.class)
public class IgniteSpiDiscoverySelfTestSuite {
    /**
     * @return Discovery SPI tests suite.
     */
    public static TestSuite suite() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteSpiDiscoverySelfTestSuite.class));

        TestSuite suite = new TestSuite("Ignite Discovery SPI Test Suite");

        // Tcp.
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryVmIpFinderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySharedFsIpFinderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryJdbcIpFinderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryMulticastIpFinderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryIpFinderCleanerTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiRandomStartStopTest.class));
        //suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiSslSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiWildcardSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiFailureTimeoutSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiMBeanTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySnapshotHistoryTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpSpiForwardingSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoverySpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(LongClientConnectToClusterTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoverySpiCoordinatorChangeTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoverySpiMulticastTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoverySpiFailureTimeoutSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpClientDiscoveryUnresolvedHostTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryNodeConsistentIdSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryNodeConfigConsistentIdSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryMultiThreadedTest.class));
        //suite.addTest(new JUnit4TestAdapter(TcpDiscoveryConcurrentStartTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySegmentationPolicyTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryNodeAttributesUpdateOnReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(AuthenticationRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryWithWrongServerTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySpiReconnectDelayTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteDiscoveryMassiveNodeFailTest.class));

        // Client connect.
        suite.addTest(new JUnit4TestAdapter(IgniteClientConnectTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteClientReconnectMassiveShutdownTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryClientSuspensionSelfTest.class));

        // SSL.
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySslSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySslTrustedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySslSecuredUnsecuredTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySslTrustedUntrustedTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoverySslParametersTest.class));

        // Disco cache reuse.
        suite.addTest(new JUnit4TestAdapter(IgniteDiscoveryCacheReuseSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(DiscoveryUnmarshalVulnerabilityTest.class));

        suite.addTest(new JUnit4TestAdapter(FilterDataForClientNodeDiscoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryPendingMessageDeliveryTest.class));

        return suite;
    }
}
