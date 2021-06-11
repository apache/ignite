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

import org.apache.ignite.common.NodeSslConnectionMetricTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite includes all test that concern REST processors.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    RouterFactorySelfTest.class,
//
//    // Parser standalone test.
//    TcpRestParserSelfTest.class,
//
//    // Test memcache protocol with custom test client.
//    RestMemcacheProtocolSelfTest.class,
//
//    // Test custom binary protocol with test client.
//    RestBinaryProtocolSelfTest.class,
//    TcpRestUnmarshalVulnerabilityTest.class,
//
//    // Test jetty rest processor
//    JettyRestProcessorSignedSelfTest.class,
//    JettyRestProcessorUnsignedSelfTest.class,
//    JettyRestProcessorAuthenticationWithCredsSelfTest.class,
//    JettyRestProcessorAuthenticationWithTokenSelfTest.class,
//    JettyRestProcessorAuthenticatorUserManagementAuthorizationTest.class,
//    JettyRestProcessorGetAllAsArrayTest.class,
//    JettyRestProcessorBaselineSelfTest.class,
//    JettyRestProcessorBeforeNodeStartSelfTest.class,
//
//    // Test TCP rest processor with original memcache client.
//    ClientMemcachedProtocolSelfTest.class,
//
//    // Test TCP rest processor with original REDIS client.
//    RedisProtocolStringSelfTest.class,
//    RedisProtocolGetAllAsArrayTest.class,
//    RedisProtocolConnectSelfTest.class,
//    RedisProtocolServerSelfTest.class,
//    RedisProtocolStringAtomicDatastructuresSelfTest.class,
//
//    RestProcessorStartSelfTest.class,
//
//    // Test cache flag conversion.
//    ClientCacheFlagsCodecTest.class,
//
//    // Test multi-start.
//    RestProcessorMultiStartSelfTest.class,
//
//    // Test clients.
//    ClientDataImplSelfTest.class,
//    ClientComputeImplSelfTest.class,
//    ClientTcpSelfTest.class,
//    ClientTcpDirectSelfTest.class,
//    ClientTcpSslSelfTest.class,
//    ClientTcpSslDirectSelfTest.class,
//
//    // Test client with many nodes.
//    ClientTcpMultiNodeSelfTest.class,
//    ClientTcpDirectMultiNodeSelfTest.class,
//    ClientTcpSslMultiNodeSelfTest.class,
//    ClientTcpSslDirectMultiNodeSelfTest.class,
//    ClientTcpUnreachableMultiNodeSelfTest.class,
//    ClientPreferDirectSelfTest.class,
//
//    //Test REST probe cmd
//    GridProbeCommandTest.class,
//
//    // Test client with many nodes and in multithreaded scenarios
//    ClientTcpMultiThreadedSelfTest.class,
//    ClientTcpSslMultiThreadedSelfTest.class,
//
//    // Test client authentication.
//    ClientTcpSslAuthenticationSelfTest.class,
//
//    ClientTcpConnectivitySelfTest.class,
//    ClientReconnectionSelfTest.class,
//
//    // Rest task command handler test.
//    TaskCommandHandlerSelfTest.class,
//    ChangeStateCommandHandlerTest.class,
//    TaskEventSubjectIdSelfTest.class,
//
//    // Default cache only test.
//    ClientDefaultCacheSelfTest.class,
//
//    ClientFutureAdapterSelfTest.class,
//    ClientPropertiesConfigurationSelfTest.class,
//    ClientConsistentHashSelfTest.class,
//    ClientJavaHasherSelfTest.class,
//
//    ClientByteUtilsTest.class,
//
//    // Router tests.
//    TcpRouterSelfTest.class,
//    TcpSslRouterSelfTest.class,
//    TcpRouterMultiNodeSelfTest.class,
//
//    ClientFailedInitSelfTest.class,
//
//    ClientTcpTaskExecutionAfterTopologyRestartSelfTest.class,
//
//    // SSL params.
//    ClientSslParametersTest.class,
//
//    IgniteClientFailuresTest.class,
//
//    ClientSizeCacheCreationDestructionTest.class,
//    ClientSideCacheCreationDestructionWileTopologyChangeTest.class,
    NodeSslConnectionMetricTest.class
})
public class IgniteClientTestSuite {
}
