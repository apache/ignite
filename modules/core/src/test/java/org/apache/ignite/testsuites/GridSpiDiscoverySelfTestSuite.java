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
import org.gridgain.grid.spi.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

/**
 * Test suite for all discovery spi implementations.
 */
public class GridSpiDiscoverySelfTestSuite extends TestSuite {
    /**
     * @return Discovery SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Discovery SPI Test Suite");

        // Tcp.
        suite.addTest(new TestSuite(GridTcpDiscoveryVmIpFinderSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoverySharedFsIpFinderSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoveryJdbcIpFinderSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoveryMulticastIpFinderSelfTest.class));

        suite.addTest(new TestSuite(GridTcpDiscoverySelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoverySpiSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoverySpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoverySpiConfigSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoveryMarshallerCheckSelfTest.class));
        suite.addTest(new TestSuite(GridTcpDiscoverySnapshotHistoryTest.class));

        suite.addTest(new TestSuite(GridTcpSpiForwardingSelfTest.class));

        return suite;
    }
}
