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
import org.apache.ignite.spi.GridTcpSpiForwardingSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiMulticastTest;
import org.apache.ignite.spi.discovery.tcp.TcpClientDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMarshallerCheckSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryMultiThreadedTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConfigConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryNodeConsistentIdSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoveryRestartTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySnapshotHistoryTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiConfigSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiFailureTimeoutSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpiStartStopSelfTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySslSelfTest;
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
        suite.addTest(new TestSuite(TcpDiscoverySpiFailureTimeoutSelfTest.class));
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

        // SSL.
        suite.addTest(new TestSuite(TcpDiscoverySslSelfTest.class));

        return suite;
    }
}
