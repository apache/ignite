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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Tests logging in {@link TcpCommunicationSpi} in relation to 'node left' event.
 */
public class TcpCommunicationSpiNodeLeftLoggingTest extends GridCommonAbstractTest {
    /***/
    private static final String SERVER1_NAME = "server1";

    /***/
    private static final String CLIENT_NAME = "client";

    /***/
    private final MemorizingAppender log4jAppender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        cfg.setCommunicationSpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        log4jAppender.installSelfOn(TcpCommunicationSpi.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        log4jAppender.removeSelfFrom(TcpCommunicationSpi.class);

        stopAllGrids();

        super.afterTest();
    }

    /**
     * Tests that when we cannot send a message to a server node that left the topology, then we log this at INFO level.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void logsWithErrorWhenCantSendMessageToServerWhichLeft() throws Exception {
        IgniteEx server1 = startGrid(SERVER1_NAME);
        IgniteEx server2 = startGrid("server2");

        ClusterNode server2Node = server2.localNode();

        server1.cluster().state(ACTIVE);

        server2.close();

        sendFailingMessage(server1, server2Node);

        LogEvent event = log4jAppender.singleEventSatisfying(
            evt -> evt.getMessage().getFormattedMessage().startsWith("Failed to send message to remote node")
        );

        assertThat(event.getLevel(), is(Level.ERROR));
    }

    /**
     * Sends some message from one Ignite node to another node, the send will fail because the target node
     * has already left.
     *
     * @param sourceIgnite Ignite node from which to send a message.
     * @param targetNode   Target node to which to send the message.
     */
    private void sendFailingMessage(Ignite sourceIgnite, ClusterNode targetNode) {
        GridTestUtils.assertThrows(
            log,
            () -> sourceIgnite.configuration().getCommunicationSpi().sendMessage(targetNode, someMessage()),
            Exception.class,
            null
        );
    }

    /**
     * Returns some message.
     *
     * @return Some message.
     */
    private UUIDCollectionMessage someMessage() {
        return new UUIDCollectionMessage(singletonList(UUID.randomUUID()));
    }

    /**
     * Tests that when we cannot send a message to a client node that left the topology, then we log this at WARN level.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    public void logsWithWarnWhenCantSendMessageToClientWhichLeft() throws Exception {
        IgniteEx server = startGrid(SERVER1_NAME);
        IgniteEx client = startClientGrid(CLIENT_NAME);

        ClusterNode clientNode = client.localNode();

        server.cluster().state(ACTIVE);

        client.close();

        sendFailingMessage(server, clientNode);

        LogEvent event = log4jAppender.singleEventSatisfying(
            evt -> evt.getMessage().getFormattedMessage().startsWith("Failed to send message to remote node")
        );

        assertThat(event.getLevel(), is(Level.WARN));
    }
}
