/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationConnectOnInitTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitSslTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationHandshakeWaitTest;
import org.apache.ignite.spi.communication.tcp.IgniteTcpCommunicationRecoveryAckClosureSelfTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiDropNodesTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientSslTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFaultyClientTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiFreezingClientTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiHalfOpenedConnectionTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiSkipMessageSendTest;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationStatisticsTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for all communication SPIs.
 */
@RunWith(AllTests.class)
public class IgniteSpiCommunicationSelfTestSuite {
    /**
     * @return Communication SPI tests suite.
     */
    public static TestSuite suite() {
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
        suite.addTest(new JUnit4TestAdapter(IgniteTcpCommunicationConnectOnInitTest.class));

        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiFreezingClientTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiFaultyClientTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpCommunicationSpiFaultyClientSslTest.class));



        //suite.addTest(new TestSuite(GridCacheDhtLockBackupSelfTest.class));

        return suite;
    }
}
