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

package org.apache.ignite.internal.client.suite;

import junit.framework.TestSuite;
import org.apache.ignite.internal.client.ClientDefaultCacheSelfTest;
import org.apache.ignite.internal.client.ClientReconnectionSelfTest;
import org.apache.ignite.internal.client.ClientTcpMultiThreadedSelfTest;
import org.apache.ignite.internal.client.ClientTcpSslAuthenticationSelfTest;
import org.apache.ignite.internal.client.ClientTcpSslMultiThreadedSelfTest;
import org.apache.ignite.internal.client.ClientTcpTaskExecutionAfterTopologyRestartSelfTest;
import org.apache.ignite.internal.client.impl.ClientCacheFlagsCodecTest;
import org.apache.ignite.internal.client.impl.ClientComputeImplSelfTest;
import org.apache.ignite.internal.client.impl.ClientDataImplSelfTest;
import org.apache.ignite.internal.client.impl.ClientFutureAdapterSelfTest;
import org.apache.ignite.internal.client.impl.ClientPropertiesConfigurationSelfTest;
import org.apache.ignite.internal.client.integration.ClientPreferDirectSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpConnectivitySelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpDirectMultiNodeSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpDirectSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpMultiNodeSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpSslDirectMultiNodeSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpSslDirectSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpSslMultiNodeSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpSslSelfTest;
import org.apache.ignite.internal.client.integration.ClientTcpUnreachableMultiNodeSelfTest;
import org.apache.ignite.internal.client.router.ClientFailedInitSelfTest;
import org.apache.ignite.internal.client.router.RouterFactorySelfTest;
import org.apache.ignite.internal.client.router.TcpRouterMultiNodeSelfTest;
import org.apache.ignite.internal.client.router.TcpRouterSelfTest;
import org.apache.ignite.internal.client.router.TcpSslRouterSelfTest;
import org.apache.ignite.internal.client.util.ClientByteUtilsTest;
import org.apache.ignite.internal.client.util.ClientConsistentHashSelfTest;
import org.apache.ignite.internal.client.util.ClientJavaHasherSelfTest;
import org.apache.ignite.internal.processors.rest.ClientMemcachedProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorSignedSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorUnsignedSelfTest;
import org.apache.ignite.internal.processors.rest.RestBinaryProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.RestMemcacheProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.RestProcessorMultiStartSelfTest;
import org.apache.ignite.internal.processors.rest.RestProcessorStartSelfTest;
import org.apache.ignite.internal.processors.rest.TaskCommandHandlerSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.TcpRestParserSelfTest;

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
        suite.addTest(new TestSuite(ClientTcpSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpDirectSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpSslSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpSslDirectSelfTest.class));

        // Test client with many nodes.
        suite.addTest(new TestSuite(ClientTcpMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpSslMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpSslDirectMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpUnreachableMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(ClientPreferDirectSelfTest.class));

        // Test client with many nodes and in multithreaded scenarios
        suite.addTest(new TestSuite(ClientTcpMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(ClientTcpSslMultiThreadedSelfTest.class));

        // Test client authentication.
        suite.addTest(new TestSuite(ClientTcpSslAuthenticationSelfTest.class));

        suite.addTest(new TestSuite(ClientTcpConnectivitySelfTest.class));
        suite.addTest(new TestSuite(ClientReconnectionSelfTest.class));

        // Rest task command handler test.
        suite.addTest(new TestSuite(TaskCommandHandlerSelfTest.class));

        // Default cache only test.
        suite.addTest(new TestSuite(ClientDefaultCacheSelfTest.class));

        suite.addTestSuite(ClientFutureAdapterSelfTest.class);
        suite.addTestSuite(ClientPropertiesConfigurationSelfTest.class);
        suite.addTestSuite(ClientConsistentHashSelfTest.class);
        suite.addTestSuite(ClientJavaHasherSelfTest.class);

        suite.addTestSuite(ClientByteUtilsTest.class);

        // Router tests.
        suite.addTest(new TestSuite(TcpRouterSelfTest.class));
        suite.addTest(new TestSuite(TcpSslRouterSelfTest.class));
        suite.addTest(new TestSuite(TcpRouterMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(ClientFailedInitSelfTest.class));

        suite.addTest(new TestSuite(ClientTcpTaskExecutionAfterTopologyRestartSelfTest.class));

        return suite;
    }
}