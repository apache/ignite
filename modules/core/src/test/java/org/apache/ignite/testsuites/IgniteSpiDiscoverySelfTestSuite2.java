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

import org.apache.ignite.spi.ExponentialBackoffTimeoutStrategyTest;
import org.apache.ignite.spi.discovery.DiscoverySpiDataExchangeTest;
import org.apache.ignite.spi.discovery.FilterDataForClientNodeDiscoveryTest;
import org.apache.ignite.spi.discovery.IgniteClientReconnectEventHandlingTest;
import org.apache.ignite.spi.discovery.IgniteDiscoveryCacheReuseSelfTest;
import org.apache.ignite.spi.discovery.LongClientConnectToClusterTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryDeserializationExceptionTest;
import org.apache.ignite.spi.discovery.tcp.DiscoveryUnmarshalVulnerabilityTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientConnectSslTest;
import org.apache.ignite.spi.discovery.tcp.IgniteClientReconnectMassiveShutdownTest;
import org.apache.ignite.spi.discovery.tcp.IgniteMetricsOverflowTest;
import org.apache.ignite.spi.discovery.tcp.MultiDataCenterRingTest;
import org.apache.ignite.spi.discovery.tcp.MultiDataCenterSplitTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiCoordinatorChangeTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryUnresolvedHostTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryConcurrentStartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryDeadNodeAddressResolvingTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryIpFinderFailureTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMetricsWarnLogTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNetworkIssuesTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConfigConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryPendingMessageDeliveryMdcReversedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryRestartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySnapshotHistoryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiMBeanTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslTrustedUntrustedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryWithAddressFilterTest;
import org.apache.ignite.spi.discovery.tcp.TestMetricUpdateFailure;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinderSelfTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Split off from {@link IgniteSpiDiscoverySelfTestSuite} to reduce the single-suite runtime in CI;
 * contains an independent subset of the same test classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TcpDiscoveryMulticastIpFinderSelfTest.class,
    TcpDiscoverySelfTest.class,
    TcpDiscoverySpiSslSelfTest.class,
    TcpDiscoverySpiFailureTimeoutSelfTest.class,
    TcpDiscoverySpiMBeanTest.class,
    TcpDiscoverySnapshotHistoryTest.class,
    ExponentialBackoffTimeoutStrategyTest.class,
    TcpClientDiscoverySpiSelfTest.class,
    LongClientConnectToClusterTest.class,
    TcpClientDiscoverySpiCoordinatorChangeTest.class,
    TcpClientDiscoveryUnresolvedHostTest.class,
    TcpDiscoveryNodeConsistentIdSelfTest.class,
    TcpDiscoveryNodeConfigConsistentIdSelfTest.class,
    TcpDiscoveryRestartTest.class,
    TcpDiscoveryMetricsWarnLogTest.class,
    TcpDiscoveryConcurrentStartTest.class,
    TcpDiscoveryWithAddressFilterTest.class,
    TcpDiscoveryNetworkIssuesTest.class,
    TestMetricUpdateFailure.class,
    IgniteClientConnectSslTest.class,
    IgniteClientReconnectMassiveShutdownTest.class,
    IgniteClientReconnectEventHandlingTest.class,
    TcpDiscoverySslSelfTest.class,
    TcpDiscoverySslTrustedUntrustedTest.class,
    IgniteDiscoveryCacheReuseSelfTest.class,
    DiscoveryUnmarshalVulnerabilityTest.class,
    FilterDataForClientNodeDiscoveryTest.class,
    IgniteMetricsOverflowTest.class,
    DiscoverySpiDataExchangeTest.class,
    TcpDiscoveryIpFinderFailureTest.class,
    TcpDiscoveryDeadNodeAddressResolvingTest.class,
    DiscoveryDeserializationExceptionTest.class,
    TcpDiscoveryPendingMessageDeliveryMdcReversedTest.class,
    MultiDataCenterRingTest.class,
    MultiDataCenterSplitTest.class,
})
public class IgniteSpiDiscoverySelfTestSuite2 {
}
