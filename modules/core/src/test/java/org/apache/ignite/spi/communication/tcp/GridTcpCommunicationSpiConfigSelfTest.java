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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.getFreeCommPort;

/**
 * TCP communication SPI config test.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpCommunicationSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 65636);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPortRange", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "idleConnectionTimeout", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketReceiveBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketSendBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "messageQueueLimit", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "selectorsCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "maxConnectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketWriteTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "unacknowledgedMessagesBufferSize", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", Integer.MAX_VALUE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPortRange() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPortRange(0);
        commSpi.setLocalPort(getFreeCommPort());

        cfg.setCommunicationSpi(commSpi);

        startGrid(cfg);
    }

}
