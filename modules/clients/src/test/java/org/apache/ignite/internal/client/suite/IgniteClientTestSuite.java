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
import org.apache.ignite.internal.TaskEventSubjectIdSelfTest;
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
import org.apache.ignite.internal.processors.rest.ChangeStateCommandHandlerTest;
import org.apache.ignite.internal.processors.rest.ClientMemcachedProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorAuthenticationWithCredsSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorAuthenticationWithTokenSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorSignedSelfTest;
import org.apache.ignite.internal.processors.rest.JettyRestProcessorUnsignedSelfTest;
import org.apache.ignite.internal.processors.rest.RestBinaryProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.RestMemcacheProtocolSelfTest;
import org.apache.ignite.internal.processors.rest.RestProcessorMultiStartSelfTest;
import org.apache.ignite.internal.processors.rest.RestProcessorStartSelfTest;
import org.apache.ignite.internal.processors.rest.TaskCommandHandlerSelfTest;
import org.apache.ignite.internal.processors.rest.TcpRestUnmarshalVulnerabilityTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.TcpRestParserSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolConnectSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolServerSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolStringSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite includes all test that concern REST processors.
 */
public class IgniteClientTestSuite extends TestSuite {
    /**
     * @return Suite that contains all tests for REST.
     */
    public static TestSuite suite() {
        TestSuite suite = new IgniteTestSuite("Ignite Clients Test Suite");

        suite.addTestSuite(RouterFactorySelfTest.class);

        // Parser standalone test.
        suite.addTestSuite(TcpRestParserSelfTest.class);

        // Test memcache protocol with custom test client.
        suite.addTestSuite(RestMemcacheProtocolSelfTest.class);

        // Test custom binary protocol with test client.
        suite.addTestSuite(RestBinaryProtocolSelfTest.class);
        suite.addTestSuite(TcpRestUnmarshalVulnerabilityTest.class);

        // Test jetty rest processor
        suite.addTestSuite(JettyRestProcessorSignedSelfTest.class);
        suite.addTestSuite(JettyRestProcessorUnsignedSelfTest.class);
        suite.addTestSuite(JettyRestProcessorAuthenticationWithCredsSelfTest.class);
        suite.addTestSuite(JettyRestProcessorAuthenticationWithTokenSelfTest.class);

        // Test TCP rest processor with original memcache client.
        suite.addTestSuite(ClientMemcachedProtocolSelfTest.class);

        // Test TCP rest processor with original REDIS client.
        suite.addTestSuite(RedisProtocolStringSelfTest.class);
        suite.addTestSuite(RedisProtocolConnectSelfTest.class);
        suite.addTestSuite(RedisProtocolServerSelfTest.class);

        suite.addTestSuite(RestProcessorStartSelfTest.class);

        // Test cache flag conversion.
        suite.addTestSuite(ClientCacheFlagsCodecTest.class);

        // Test multi-start.
        suite.addTestSuite(RestProcessorMultiStartSelfTest.class);

        // Test clients.
        suite.addTestSuite(ClientDataImplSelfTest.class);
        suite.addTestSuite(ClientComputeImplSelfTest.class);
        suite.addTestSuite(ClientTcpSelfTest.class);
        suite.addTestSuite(ClientTcpDirectSelfTest.class);
        suite.addTestSuite(ClientTcpSslSelfTest.class);
        suite.addTestSuite(ClientTcpSslDirectSelfTest.class);

        // Test client with many nodes.
        suite.addTestSuite(ClientTcpMultiNodeSelfTest.class);
        suite.addTestSuite(ClientTcpDirectMultiNodeSelfTest.class);
        suite.addTestSuite(ClientTcpSslMultiNodeSelfTest.class);
        suite.addTestSuite(ClientTcpSslDirectMultiNodeSelfTest.class);
        suite.addTestSuite(ClientTcpUnreachableMultiNodeSelfTest.class);
        suite.addTestSuite(ClientPreferDirectSelfTest.class);

        // Test client with many nodes and in multithreaded scenarios
        suite.addTestSuite(ClientTcpMultiThreadedSelfTest.class);
        suite.addTestSuite(ClientTcpSslMultiThreadedSelfTest.class);

        // Test client authentication.
        suite.addTestSuite(ClientTcpSslAuthenticationSelfTest.class);

        suite.addTestSuite(ClientTcpConnectivitySelfTest.class);
        suite.addTestSuite(ClientReconnectionSelfTest.class);

        // Rest task command handler test.
        suite.addTestSuite(TaskCommandHandlerSelfTest.class);
        suite.addTestSuite(ChangeStateCommandHandlerTest.class);
        suite.addTestSuite(TaskEventSubjectIdSelfTest.class);

        // Default cache only test.
        suite.addTestSuite(ClientDefaultCacheSelfTest.class);

        suite.addTestSuite(ClientFutureAdapterSelfTest.class);
        suite.addTestSuite(ClientPropertiesConfigurationSelfTest.class);
        suite.addTestSuite(ClientConsistentHashSelfTest.class);
        suite.addTestSuite(ClientJavaHasherSelfTest.class);

        suite.addTestSuite(ClientByteUtilsTest.class);

        // Router tests.
        suite.addTestSuite(TcpRouterSelfTest.class);
        suite.addTestSuite(TcpSslRouterSelfTest.class);
        suite.addTestSuite(TcpRouterMultiNodeSelfTest.class);

        suite.addTestSuite(ClientFailedInitSelfTest.class);

        suite.addTestSuite(ClientTcpTaskExecutionAfterTopologyRestartSelfTest.class);

        return suite;
    }
}
