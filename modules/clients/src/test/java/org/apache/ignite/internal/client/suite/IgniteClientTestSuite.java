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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.TaskEventSubjectIdSelfTest;
import org.apache.ignite.internal.client.ClientDefaultCacheSelfTest;
import org.apache.ignite.internal.client.ClientReconnectionSelfTest;
import org.apache.ignite.internal.client.ClientSslParametersTest;
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
import org.apache.ignite.internal.processors.rest.JettyRestProcessorGetAllAsArrayTest;
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
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolGetAllAsArrayTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolServerSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolStringAtomicDatastructuresSelfTest;
import org.apache.ignite.internal.processors.rest.protocols.tcp.redis.RedisProtocolStringSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite includes all test that concern REST processors.
 */
@RunWith(AllTests.class)
public class IgniteClientTestSuite {
    /**
     * @return Suite that contains all tests for REST.
     */
    public static TestSuite suite() {
        TestSuite suite = new IgniteTestSuite("Ignite Clients Test Suite");

        suite.addTest(new JUnit4TestAdapter(RouterFactorySelfTest.class));

        // Parser standalone test.
        suite.addTest(new JUnit4TestAdapter(TcpRestParserSelfTest.class));

        // Test memcache protocol with custom test client.
        suite.addTest(new JUnit4TestAdapter(RestMemcacheProtocolSelfTest.class));

        // Test custom binary protocol with test client.
        suite.addTest(new JUnit4TestAdapter(RestBinaryProtocolSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpRestUnmarshalVulnerabilityTest.class));

        // Test jetty rest processor
        suite.addTest(new JUnit4TestAdapter(JettyRestProcessorSignedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JettyRestProcessorUnsignedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JettyRestProcessorAuthenticationWithCredsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JettyRestProcessorAuthenticationWithTokenSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JettyRestProcessorGetAllAsArrayTest.class));

        // Test TCP rest processor with original memcache client.
        suite.addTest(new JUnit4TestAdapter(ClientMemcachedProtocolSelfTest.class));

        // Test TCP rest processor with original REDIS client.
        suite.addTest(new JUnit4TestAdapter(RedisProtocolStringSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RedisProtocolGetAllAsArrayTest.class));
        suite.addTest(new JUnit4TestAdapter(RedisProtocolConnectSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RedisProtocolServerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(RedisProtocolStringAtomicDatastructuresSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(RestProcessorStartSelfTest.class));

        // Test cache flag conversion.
        suite.addTest(new JUnit4TestAdapter(ClientCacheFlagsCodecTest.class));

        // Test multi-start.
        suite.addTest(new JUnit4TestAdapter(RestProcessorMultiStartSelfTest.class));

        // Test clients.
        suite.addTest(new JUnit4TestAdapter(ClientDataImplSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientComputeImplSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpDirectSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslDirectSelfTest.class));

        // Test client with many nodes.
        suite.addTest(new JUnit4TestAdapter(ClientTcpMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpDirectMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslDirectMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpUnreachableMultiNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientPreferDirectSelfTest.class));

        // Test client with many nodes and in multithreaded scenarios
        suite.addTest(new JUnit4TestAdapter(ClientTcpMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslMultiThreadedSelfTest.class));

        // Test client authentication.
        suite.addTest(new JUnit4TestAdapter(ClientTcpSslAuthenticationSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientTcpConnectivitySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientReconnectionSelfTest.class));

        // Rest task command handler test.
        suite.addTest(new JUnit4TestAdapter(TaskCommandHandlerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ChangeStateCommandHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(TaskEventSubjectIdSelfTest.class));

        // Default cache only test.
        suite.addTest(new JUnit4TestAdapter(ClientDefaultCacheSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientFutureAdapterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientPropertiesConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientConsistentHashSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClientJavaHasherSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientByteUtilsTest.class));

        // Router tests.
        suite.addTest(new JUnit4TestAdapter(TcpRouterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpSslRouterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpRouterMultiNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientFailedInitSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(ClientTcpTaskExecutionAfterTopologyRestartSelfTest.class));

        // SSL params.
        suite.addTestSuite(ClientSslParametersTest.class);

        return suite;
    }
}
