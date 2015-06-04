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

import junit.framework.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

/**
 * Test suite for all discovery spi implementations.
 */
public class IgniteSpiDiscoverySelfTestSuite extends TestSuite {
    /**
     * @return Discovery SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Discovery SPI Test Suite");

        // Tcp.
        suite.addTest(new TestSuite(TcpDiscoveryVmIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySharedFsIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryJdbcIpFinderSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryMulticastIpFinderSelfTest.class));

        suite.addTest(new TestSuite(TcpDiscoverySelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySpiConfigSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new TestSuite(TcpDiscoverySnapshotHistoryTest.class));

        suite.addTest(new TestSuite(GridTcpSpiForwardingSelfTest.class));

        suite.addTest(new TestSuite(TcpClientDiscoverySpiSelfTest.class));
        suite.addTest(new TestSuite(TcpClientDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new TestSuite(TcpClientDiscoverySpiMulticastTest.class));

        return suite;
    }
}
