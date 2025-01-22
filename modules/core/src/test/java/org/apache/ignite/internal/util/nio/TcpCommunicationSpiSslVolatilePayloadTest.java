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

package org.apache.ignite.internal.util.nio;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridAbstractCommunicationSelfTest;
import org.apache.ignite.spi.communication.TestVolatilePayloadMessage;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.GridNioServerWrapper;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class TcpCommunicationSpiSslVolatilePayloadTest extends GridAbstractCommunicationSelfTest<CommunicationSpi<Message>> {
    /** */
    private static final AtomicInteger msgCreatedCntr = new AtomicInteger();

    /** */
    private static final AtomicInteger msgReceivedCntr = new AtomicInteger();

    /** */
    private static final Map<Integer, TestVolatilePayloadMessage> messages = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected CommunicationSpi<Message> getSpi(int idx) {
        return new TcpCommunicationSpi().setLocalPort(GridTestUtils.getNextCommPort(getClass()))
            .setIdleConnectionTimeout(2000)
            .setTcpNoDelay(true);
    }

    /** {@inheritDoc} */
    @Override protected CommunicationListener<Message> createMessageListener(UUID nodeId) {
        return new TestCommunicationListener();
    }

    /** {@inheritDoc} */
    @Override protected Map<Short, Supplier<Message>> customMessageTypes() {
        return Collections.singletonMap(TestVolatilePayloadMessage.DIRECT_TYPE, TestVolatilePayloadMessage::new);
    }

    /** {@inheritDoc} */
    @Override protected boolean isSslEnabled() {
        return true;
    }

    /** */
    @Test
    public void test() throws Exception {
        ClusterNode from = nodes.get(0);
        ClusterNode to = nodes.get(1);

        for (int i = 0; i < 1000; i++) {
            // Force connection to be established.
            sendMessage(from, to, createMessage());

            GridNioRecoveryDescriptor fromDesc = extractDescriptor(from);
            GridNioRecoveryDescriptor toDesc = extractDescriptor(to);

            // Stores multiple dummy messages in a recovery descriptor. When the connection is restored, they will be
            // written to the network buffer along with the last handshake message.
            // See TcpHandshakeExecutor#receiveAcknowledge
            for (int j = 0; j < 50; j++)
                toDesc.add(new GridNioServer.WriteRequestImpl(toDesc.session(), createMessage(), false, null));

            fromDesc.session().close();
        }

        assertTrue(waitForCondition(() -> msgCreatedCntr.get() == msgReceivedCntr.get(), 5000));
    }

    /** */
    public GridNioRecoveryDescriptor extractDescriptor(ClusterNode node) throws Exception {
        CommunicationSpi<Message> spi = spis.get(node.id());

        GridNioServerWrapper wrapper = U.field(spi, "nioSrvWrapper");

        assertTrue(waitForCondition(() -> !wrapper.recoveryDescs().values().isEmpty(), getTestTimeout()));

        return wrapper.recoveryDescs().values().stream().findFirst().get();
    }

    /** */
    private Message createMessage() {
        byte[] payload = new byte[ThreadLocalRandom.current().nextInt(10, 1024)];

        ThreadLocalRandom.current().nextBytes(payload);

        TestVolatilePayloadMessage msg = new TestVolatilePayloadMessage(msgCreatedCntr.getAndIncrement(), payload);

        messages.put(msg.index(), msg);

        return msg;
    }

    /** */
    private void sendMessage(ClusterNode from, ClusterNode to, Message msg) {
        spis.get(from.id()).sendMessage(to, msg);
    }

    /** */
    private static class TestCommunicationListener implements CommunicationListener<Message> {
        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Message msg, IgniteRunnable msgC) {
            msgC.run();

            if (msg instanceof TestVolatilePayloadMessage) {
                TestVolatilePayloadMessage testMsg = (TestVolatilePayloadMessage)msg;

                TestVolatilePayloadMessage expMsg = messages.get(testMsg.index());

                assertNotNull(expMsg);

                assertTrue(Arrays.equals(expMsg.payload(), testMsg.payload()));

                msgReceivedCntr.incrementAndGet();
            }
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(UUID nodeId) {
            // No-op.
        }
    }
}
