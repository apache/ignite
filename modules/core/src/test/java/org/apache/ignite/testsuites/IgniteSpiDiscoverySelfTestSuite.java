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
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.AuthenticationRestartTest;
import org.apache.ignite.spi.discovery.datacenter.MultiDataCenterClientRoutingTest;
import org.apache.ignite.spi.discovery.datacenter.MultiDataCenterDeploymentTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryClientSocketTest;
import org.apache.ignite.spi.discovery.tcp.DiscoverySerializationExceptionTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownSslTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryClientSuspensionSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryCoordinatorFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryFailedJoinTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderCleanerTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMdcSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMultiThreadedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeAttributesUpdateOnReconnectTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeJoinAndFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryMdcTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryReconnectUnstableTopologyTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySegmentationPolicyTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiConfigSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiReconnectDelayTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiStartStopSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiWildcardSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslParametersTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSecuredUnsecuredTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryWithWrongServerTest;

import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinderDnsResolveTest;
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
    TcpDiscoveryVmIpFinderDnsResolveTest.class,
    TcpDiscoveryVmIpFinderSelfTest.class,

    TcpDiscoveryIpFinderCleanerTest.class,

    TcpDiscoverySpiSelfTest.class,
    TcpDiscoverySpiWildcardSelfTest.class,
    TcpDiscoverySpiStartStopSelfTest.class,
    TcpDiscoverySpiConfigSelfTest.class,
    TcpDiscoveryNodeJoinAndFailureTest.class,

    GridTcpSpiForwardingSelfTest.class,

    TcpClientDiscoveryMarshallerCheckSelfTest.class,
    TcpClientDiscoverySpiMulticastTest.class,
    TcpClientDiscoverySpiFailureTimeoutSelfTest.class,

    TcpDiscoveryMultiThreadedTest.class,

    TcpDiscoverySegmentationPolicyTest.class,

    TcpDiscoveryNodeAttributesUpdateOnReconnectTest.class,
    AuthenticationRestartTest.class,

    TcpDiscoveryWithWrongServerTest.class,

    TcpDiscoverySpiReconnectDelayTest.class,

    IgniteDiscoveryMassiveNodeFailTest.class,
    TcpDiscoveryCoordinatorFailureTest.class,

    // Client connect.
    IgniteClientConnectTest.class,
    IgniteClientReconnectMassiveShutdownSslTest.class,
    TcpDiscoveryClientSuspensionSelfTest.class,

    TcpDiscoveryFailedJoinTest.class,

    // SSL.
    TcpDiscoverySslTrustedSelfTest.class,
    TcpDiscoverySslSecuredUnsecuredTest.class,
    TcpDiscoverySslParametersTest.class,

    // Disco cache reuse.

    TcpDiscoveryPendingMessageDeliveryTest.class,

    TcpDiscoveryReconnectUnstableTopologyTest.class,

    DiscoveryClientSocketTest.class,

    DiscoverySerializationExceptionTest.class,

    // MDC.
    TcpDiscoveryMdcSelfTest.class,
    TcpDiscoveryPendingMessageDeliveryMdcTest.class,
    MultiDataCenterDeploymentTest.class,
    MultiDataCenterClientRoutingTest.class
})
public class IgniteSpiDiscoverySelfTestSuite {
    /** */
    @BeforeClass
    public static void init() {
        System.setProperty(IGNITE_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(IgniteSpiDiscoverySelfTestSuite.class));
    }
}
