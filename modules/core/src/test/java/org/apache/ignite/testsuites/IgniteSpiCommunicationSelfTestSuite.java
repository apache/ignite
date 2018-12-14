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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiSslSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiSslSmallBuffersSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiStartStopSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpFailureDetectionSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpNoDelayOffSelfTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiTcpSelfTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitSslTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationRecoveryAckClosureSelfTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiDropNodesTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiHalfOpenedConnectionTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiSkipMessageSendTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationStatisticsTest;

/**
 * Test suite for all communication SPIs.
 */
public class IgniteSpiCommunicationSelfTestSuite extends TestSuite {
    /**
     * @return Communication SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Communication SPI Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiRecoveryAckSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTcpCommunicationRecoveryAckClosureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiRecoverySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiRecoveryNoPairedConnectionsTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiRecoverySslSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiConcurrentConnectSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiConcurrentConnectSslSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiSslSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiSslSmallBuffersSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiTcpSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiTcpNoDelayOffSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiShmemSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiStartStopSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiMultithreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiMultithreadedShmemTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiRecoveryFailureDetectionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiTcpFailureDetectionSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridTcpCommunicationSpiConfigSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiSkipMessageSendTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiFaultyClientTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiDropNodesTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiHalfOpenedConnectionTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpCommunicationStatisticsTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteTcpCommunicationHandshakeWaitTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteTcpCommunicationHandshakeWaitSslTest.class));

        //suite.addTest(new TestSuite(GridCacheDhtLockBackupSelfTest.class));

        return suite;
    }
}
