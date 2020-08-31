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

import org.apache.ignite.spi.communication.tcp.GridSandboxedClientWithoutNetworkTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationInverseConnectionEstablishingTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiConcurrentConnectSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiConcurrentConnectSslSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiConfigSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiMultithreadedSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiMultithreadedShmemTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiRecoveryAckSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiRecoveryFailureDetectionSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiRecoveryNoPairedConnectionsTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiRecoverySelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiRecoverySslSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiShmemSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiSkipWaitHandshakeOnClientTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiSslSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiSslSmallBuffersSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiStartStopSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpFailureDetectionSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpNoDelayOffSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTotallyUnreachableClientTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationConnectOnInitTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitSslTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationRecoveryAckClosureSelfTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiDropNodesTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientSslTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFreezingClientTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiHalfOpenedConnectionTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMultiJvmTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiSkipMessageSendTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationStatisticsTest;
import org.apache.ignite.spi.communication.tcp.TooManyOpenFilesTcpCommunicationSpiTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all communication SPIs.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridTcpCommunicationSpiRecoveryAckSelfTest.class,
    IgniteTcpCommunicationRecoveryAckClosureSelfTest.class,
    GridTcpCommunicationSpiRecoverySelfTest.class,
    GridTcpCommunicationSpiRecoveryNoPairedConnectionsTest.class,
    GridTcpCommunicationSpiRecoverySslSelfTest.class,

    GridTcpCommunicationSpiConcurrentConnectSelfTest.class,
    GridTcpCommunicationSpiConcurrentConnectSslSelfTest.class,

    GridTcpCommunicationSpiSslSelfTest.class,
    GridTcpCommunicationSpiSslSmallBuffersSelfTest.class,

    GridTcpCommunicationSpiTcpSelfTest.class,
    GridTcpCommunicationSpiTcpNoDelayOffSelfTest.class,
    GridTcpCommunicationSpiShmemSelfTest.class,

    GridTcpCommunicationSpiStartStopSelfTest.class,

    GridTcpCommunicationSpiMultithreadedSelfTest.class,
    GridTcpCommunicationSpiMultithreadedShmemTest.class,

    GridTcpCommunicationSpiRecoveryFailureDetectionSelfTest.class,
    GridTcpCommunicationSpiTcpFailureDetectionSelfTest.class,

    GridTcpCommunicationSpiConfigSelfTest.class,

    TcpCommunicationSpiSkipMessageSendTest.class,

    TcpCommunicationSpiFaultyClientTest.class,
    TcpCommunicationSpiFaultyClientSslTest.class,

    TcpCommunicationSpiFreezingClientTest.class,

    TcpCommunicationSpiDropNodesTest.class,
    TcpCommunicationSpiHalfOpenedConnectionTest.class,
    GridTcpCommunicationSpiSkipWaitHandshakeOnClientTest.class,

    TcpCommunicationStatisticsTest.class,

    IgniteTcpCommunicationHandshakeWaitTest.class,
    IgniteTcpCommunicationHandshakeWaitSslTest.class,
    IgniteTcpCommunicationConnectOnInitTest.class,

    TcpCommunicationSpiMultiJvmTest.class,
    TooManyOpenFilesTcpCommunicationSpiTest.class,

    GridTcpCommunicationInverseConnectionEstablishingTest.class,
    GridTotallyUnreachableClientTest.class,
    GridSandboxedClientWithoutNetworkTest.class

    //GridCacheDhtLockBackupSelfTest.class,
})
public class IgniteSpiCommunicationSelfTestSuite {
}
