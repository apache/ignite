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

package org.apache.ignite.client.suite;

import junit.framework.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.impl.*;
import org.apache.ignite.client.integration.*;
import org.apache.ignite.client.router.*;
import org.apache.ignite.client.util.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.protocols.tcp.*;

/**
 * Test suite includes all test that concern REST processors.
 */
public class IgniteClientTestSuite extends TestSuite {
    /**
     * @return Suite that contains all tests for REST.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Clients Test Suite");

        suite.addTest(new TestSuite(RouterFactorySelfTest.class));

        // Parser standalone test.
        suite.addTest(new TestSuite(TcpRestParserSelfTest.class));

        // Test memcache protocol with custom test client.
        suite.addTest(new TestSuite(RestMemcacheProtocolSelfTest.class));

        // Test custom binary protocol with test client.
        suite.addTest(new TestSuite(RestBinaryProtocolSelfTest.class));

        // Test jetty rest processor
        suite.addTest(new TestSuite(JettyRestProcessorSignedSelfTest.class));
        suite.addTest(new TestSuite(JettyRestProcessorUnsignedSelfTest.class));

        // Test TCP rest processor with original memcache client.
        suite.addTest(new TestSuite(ClientMemcachedProtocolSelfTest.class));

        suite.addTest(new TestSuite(RestProcessorStartSelfTest.class));

        // Test cache flag conversion.
        suite.addTest(new TestSuite(ClientCacheFlagsCodecTest.class));

        // Test multi-start.
        suite.addTest(new TestSuite(RestProcessorMultiStartSelfTest.class));

        // Test clients.
        suite.addTest(new TestSuite(ClientDataImplSelfTest.class));
        suite.addTest(new TestSuite(ClientComputeImplSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpDirectSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslDirectSelfTest.class));

        // Test client with many nodes.
        suite.addTest(new TestSuite(GridClientTcpMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpUnreachableMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridClientPreferDirectSelfTest.class));

        // Test client with many nodes and in multithreaded scenarios
        suite.addTest(new TestSuite(GridClientTcpMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridClientTcpSslMultiThreadedSelfTest.class));

        // Test client authentication.
        suite.addTest(new TestSuite(GridClientTcpSslAuthenticationSelfTest.class));

        suite.addTest(new TestSuite(GridClientTcpConnectivitySelfTest.class));
        suite.addTest(new TestSuite(GridClientReconnectionSelfTest.class));

        // Rest task command handler test.
        suite.addTest(new TestSuite(TaskCommandHandlerSelfTest.class));

        // Default cache only test.
        suite.addTest(new TestSuite(GridClientDefaultCacheSelfTest.class));

        suite.addTestSuite(ClientFutureAdapterSelfTest.class);
        suite.addTestSuite(ClientPartitionAffinitySelfTest.class);
        suite.addTestSuite(ClientPropertiesConfigurationSelfTest.class);
        suite.addTestSuite(ClientConsistentHashSelfTest.class);
        suite.addTestSuite(ClientJavaHasherSelfTest.class);

        suite.addTestSuite(ClientByteUtilsTest.class);

        suite.addTest(new TestSuite(GridClientTopologyCacheSelfTest.class));

        // Router tests.
        suite.addTest(new TestSuite(TcpRouterSelfTest.class));
        suite.addTest(new TestSuite(TcpSslRouterSelfTest.class));
        suite.addTest(new TestSuite(TcpRouterMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(ClientFailedInitSelfTest.class));

        suite.addTest(new TestSuite(GridClientTcpTaskExecutionAfterTopologyRestartSelfTest.class));

        return suite;
    }
}
