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
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationRecoveryAckClosureSelfTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiDropNodesTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientTest;

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

        suite.addTest(new TestSuite(GridTcpCommunicationSpiRecoveryAckSelfTest.class));
        suite.addTest(new TestSuite(IgniteTcpCommunicationRecoveryAckClosureSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiRecoverySelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiRecoveryNoPairedConnectionsTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiRecoverySslSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiConcurrentConnectSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiConcurrentConnectSslSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiSslSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiSslSmallBuffersSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiTcpSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiTcpNoDelayOffSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiShmemSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiStartStopSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiMultithreadedSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiMultithreadedShmemTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiRecoveryFailureDetectionSelfTest.class));
        suite.addTest(new TestSuite(GridTcpCommunicationSpiTcpFailureDetectionSelfTest.class));

        suite.addTest(new TestSuite(GridTcpCommunicationSpiConfigSelfTest.class));

        suite.addTest(new TestSuite(TcpCommunicationSpiFaultyClientTest.class));
        suite.addTest(new TestSuite(TcpCommunicationSpiDropNodesTest.class));

        return suite;
    }
}
