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

import org.apache.ignite.internal.IgniteClientFailuresTest;
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
import org.apache.ignite.internal.processors.rest.JettyRestProcessorBaselineSelfTest;
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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite includes all test that concern REST processors.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    RouterFactorySelfTest.class,

    // Parser standalone test.
    TcpRestParserSelfTest.class,

    // Test memcache protocol with custom test client.
    RestMemcacheProtocolSelfTest.class,

    // Test custom binary protocol with test client.
    RestBinaryProtocolSelfTest.class,
    TcpRestUnmarshalVulnerabilityTest.class,

    // Test jetty rest processor
    JettyRestProcessorSignedSelfTest.class,
    JettyRestProcessorUnsignedSelfTest.class,
    JettyRestProcessorAuthenticationWithCredsSelfTest.class,
    JettyRestProcessorAuthenticationWithTokenSelfTest.class,
    JettyRestProcessorGetAllAsArrayTest.class,
    JettyRestProcessorBaselineSelfTest.class,

    // Test TCP rest processor with original memcache client.
    ClientMemcachedProtocolSelfTest.class,

    // Test TCP rest processor with original REDIS client.
    RedisProtocolStringSelfTest.class,
    RedisProtocolGetAllAsArrayTest.class,
    RedisProtocolConnectSelfTest.class,
    RedisProtocolServerSelfTest.class,
    RedisProtocolStringAtomicDatastructuresSelfTest.class,

    RestProcessorStartSelfTest.class,

    // Test cache flag conversion.
    ClientCacheFlagsCodecTest.class,

    // Test multi-start.
    RestProcessorMultiStartSelfTest.class,

    // Test clients.
    ClientDataImplSelfTest.class,
    ClientComputeImplSelfTest.class,
    ClientTcpSelfTest.class,
    ClientTcpDirectSelfTest.class,
    ClientTcpSslSelfTest.class,
    ClientTcpSslDirectSelfTest.class,

    // Test client with many nodes.
    ClientTcpMultiNodeSelfTest.class,
    ClientTcpDirectMultiNodeSelfTest.class,
    ClientTcpSslMultiNodeSelfTest.class,
    ClientTcpSslDirectMultiNodeSelfTest.class,
    ClientTcpUnreachableMultiNodeSelfTest.class,
    ClientPreferDirectSelfTest.class,

    // Test client with many nodes and in multithreaded scenarios
    ClientTcpMultiThreadedSelfTest.class,
    ClientTcpSslMultiThreadedSelfTest.class,

    // Test client authentication.
    ClientTcpSslAuthenticationSelfTest.class,

    ClientTcpConnectivitySelfTest.class,
    ClientReconnectionSelfTest.class,

    // Rest task command handler test.
    TaskCommandHandlerSelfTest.class,
    ChangeStateCommandHandlerTest.class,
    TaskEventSubjectIdSelfTest.class,

    // Default cache only test.
    ClientDefaultCacheSelfTest.class,

    ClientFutureAdapterSelfTest.class,
    ClientPropertiesConfigurationSelfTest.class,
    ClientConsistentHashSelfTest.class,
    ClientJavaHasherSelfTest.class,

    ClientByteUtilsTest.class,

    // Router tests.
    TcpRouterSelfTest.class,
    TcpSslRouterSelfTest.class,
    TcpRouterMultiNodeSelfTest.class,

    ClientFailedInitSelfTest.class,

    ClientTcpTaskExecutionAfterTopologyRestartSelfTest.class,

    // SSL params.
    ClientSslParametersTest.class,

    IgniteClientFailuresTest.class
})
public class IgniteClientTestSuite {
}
