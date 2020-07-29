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

package org.apache.ignite.spi.communication.tcp;

import java.io.InputStream;
import java.net.Socket;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.NODE_ID_MSG_TYPE;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.makeMessageType;

/**
 * This test check that client sends only Node ID message type on connect. There is a gap when {@link
 * IgniteFeatures#allNodesSupports} isn't consistent, because the list of nodes is empty.
 */
public class GridTcpCommunicationSpiSkipWaitHandshakeOnClientTest extends GridCommonAbstractTest {
    /** Tcp communication start message. */
    private final String TCP_COMM_START_MSG = "Successfully bound communication NIO server to TCP port [port=";

    /** Port number of digits. */
    private final int PORT_NUM_OF_DIGITS = 5;

    /** Local adders. */
    private static final String LOCAL_ADDERS = "127.0.0.1";

    /** Message type bytes. */
    private static final int MESSAGE_TYPE_BYTES = 2;

    /** Test logger. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridAbstractTest.log);

    /** Fetched tcp port. */
    private volatile int fetchedTcpPort = -1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(log);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void clientCanNotSendHandshakeWaitMessage() throws Exception {
        startClientAndWaitCommunicationActivation();

        assert fetchedTcpPort != -1;

        try (Socket sock = new Socket(LOCAL_ADDERS, fetchedTcpPort)) {
            try (InputStream in = sock.getInputStream()) {
                byte[] b = new byte[MESSAGE_TYPE_BYTES];

                int read = in.read(b);

                assertEquals(MESSAGE_TYPE_BYTES, read);

                short respMsgType = makeMessageType(b[0], b[1]);

                // Client can't give HANDSHAKE_WAIT_MSG_TYPE.
                Assert.assertEquals(NODE_ID_MSG_TYPE, respMsgType);
            }
        }

        startGrid(1); //This is hack because, stopAllGrids can't interrupt the client node:(
    }

    /**
     *
     */
    private void startClientAndWaitCommunicationActivation() throws IgniteInterruptedCheckedException {
        LogListener lsnr = LogListener
            .matches(msg -> {
                boolean matched = msg.startsWith(TCP_COMM_START_MSG);

                if (matched)
                    fetchedTcpPort = parsePort(msg);

                return matched;
            })
            .times(1)
            .build();
        log.registerListener(lsnr);

        new Thread(() -> {
            try {
                startGrid(0);
            }
            catch (Exception e) {
                // Noop
            }
        }).start();

        assertTrue(GridTestUtils.waitForCondition(lsnr::check, 20_000));
    }

    /**
     * @param msg Message.
     */
    private int parsePort(String msg) {
        return Integer.valueOf(
            msg.substring(TCP_COMM_START_MSG.length(), TCP_COMM_START_MSG.length() + PORT_NUM_OF_DIGITS)
        );
    }
}
