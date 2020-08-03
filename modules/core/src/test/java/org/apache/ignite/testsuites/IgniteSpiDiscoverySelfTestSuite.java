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

import org.apache.ignite.internal.IgniteDiscoveryMassiveNodeFailTest;
import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategyTest;
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.AuthenticationRestartTest;
import org.apache.ignite.spi.discovery.FilterDataForClientNodeDiscoveryTest;
import org.apache.ignite.spi.discovery.IgniteClientReconnectEventHandlingTest;
import org.apache.ignite.spi.discovery.IgniteDiscoveryCacheReuseSelfTest;
import org.apache.ignite.spi.discovery.LongClientConnectToClusterTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryClientSocketTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryUnmarshalVulnerabilityTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectSslTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownSslTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownTest;
import org.apache.ignite.spi.discovery.tcp.IgniteMetricsOverflowTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiCoordinatorChangeTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryUnresolvedHostTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientSuspensionSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryCoordinatorFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryFailedJoinTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderCleanerTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMetricsWarnLogTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMultiThreadedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNetworkIssuesTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeAttributesUpdateOnReconnectTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConfigConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeJoinAndFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryReconnectUnstableTopologyTest;
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
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_OVERRIDE_MCAST_GRP;

/**
 * Test suite for all discovery spi implementations.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TcpDiscoveryVmIpFinderSelfTest.class,
    TcpDiscoverySharedFsIpFinderSelfTest.class,
    TcpDiscoveryJdbcIpFinderSelfTest.class,
    TcpDiscoveryMulticastIpFinderSelfTest.class,
    TcpDiscoveryIpFinderCleanerTest.class,

    TcpDiscoverySelfTest.class,
    TcpDiscoverySpiSelfTest.class,
    //TcpDiscoverySpiRandomStartStopTest.class,
    //TcpDiscoverySpiSslSelfTest.class,
    //TcpDiscoverySpiWildcardSelfTest.class,
    TcpDiscoverySpiFailureTimeoutSelfTest.class,
    TcpDiscoverySpiMBeanTest.class,
    TcpDiscoverySpiStartStopSelfTest.class,
    TcpDiscoverySpiConfigSelfTest.class,
    TcpDiscoveryMarshallerCheckSelfTest.class,
    TcpDiscoverySnapshotHistoryTest.class,
    TcpDiscoveryNodeJoinAndFailureTest.class,

    GridTcpSpiForwardingSelfTest.class,

    ExponentialBackoffTimeoutStrategyTest.class,

    TcpClientDiscoverySpiSelfTest.class,
    LongClientConnectToClusterTest.class,
    TcpClientDiscoveryMarshallerCheckSelfTest.class,
    TcpClientDiscoverySpiCoordinatorChangeTest.class,
    TcpClientDiscoverySpiMulticastTest.class,
    TcpClientDiscoverySpiFailureTimeoutSelfTest.class,
    TcpClientDiscoveryUnresolvedHostTest.class,

    TcpDiscoveryNodeConsistentIdSelfTest.class,
    TcpDiscoveryNodeConfigConsistentIdSelfTest.class,

    TcpDiscoveryRestartTest.class,
    TcpDiscoveryMultiThreadedTest.class,
    TcpDiscoveryMetricsWarnLogTest.class,
    //TcpDiscoveryConcurrentStartTest.class,

    TcpDiscoverySegmentationPolicyTest.class,

    TcpDiscoveryNodeAttributesUpdateOnReconnectTest.class,
    AuthenticationRestartTest.class,

    TcpDiscoveryWithWrongServerTest.class,

    TcpDiscoverySpiReconnectDelayTest.class,

    TcpDiscoveryNetworkIssuesTest.class,

    IgniteDiscoveryMassiveNodeFailTest.class,
    TcpDiscoveryCoordinatorFailureTest.class,

    // Client connect.
    IgniteClientConnectTest.class,
    IgniteClientConnectSslTest.class,
    IgniteClientReconnectMassiveShutdownTest.class,
    IgniteClientReconnectMassiveShutdownSslTest.class,
    TcpDiscoveryClientSuspensionSelfTest.class,
    IgniteClientReconnectEventHandlingTest.class,

    TcpDiscoveryFailedJoinTest.class,

    // SSL.
    TcpDiscoverySslSelfTest.class,
    TcpDiscoverySslTrustedSelfTest.class,
    TcpDiscoverySslSecuredUnsecuredTest.class,
    TcpDiscoverySslTrustedUntrustedTest.class,
    TcpDiscoverySslParametersTest.class,

    // Disco cache reuse.
    IgniteDiscoveryCacheReuseSelfTest.class,

    DiscoveryUnmarshalVulnerabilityTest.class,

    FilterDataForClientNodeDiscoveryTest.class,

    TcpDiscoveryPendingMessageDeliveryTest.class,

    TcpDiscoveryReconnectUnstableTopologyTest.class,

    IgniteMetricsOverflowTest.class,

    DiscoveryClientSocketTest.class
})
public class IgniteSpiDiscoverySelfTestSuite {
    /** */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteSpiDiscoverySelfTestSuite.class));
    }
}
