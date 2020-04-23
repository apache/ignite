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

package org.apache.ignite.internal.managers.communication;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Send message test.
 */
public class GridCommunicationSendMessageSelfTest extends GridCommonAbstractTest {
    /** Sample count. */
    private static final int SAMPLE_CNT = 1;

    /** */
    private static final short DIRECT_TYPE = -127;

    /** */
    private static final short DIRECT_TYPE_OVER_BYTE = 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setPluginProviders(new TestPluginProvider());

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        c.setCommunicationSpi(commSpi);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendMessage() throws Exception {
        try {
            startGridsMultiThreaded(2);

            doSend(new TestMessage(), TestMessage.class);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendMessageOverByteId() throws Exception {
        try {
            startGridsMultiThreaded(2);

            doSend(new TestOverByteIdMessage(), TestOverByteIdMessage.class);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendMessageWithBuffer() throws Exception {
        try {
            startGridsMultiThreaded(2);

            doSend(new TestMessage(), TestMessage.class);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param msg Message to send.
     * @param msgCls Message class to check the received message.
     *
     * @throws Exception If failed.
     */
    private void doSend(Message msg, final Class<?> msgCls) throws Exception {
        GridIoManager mgr0 = grid(0).context().io();
        GridIoManager mgr1 = grid(1).context().io();

        String topic = "test-topic";

        final CountDownLatch latch = new CountDownLatch(SAMPLE_CNT);

        mgr1.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msgCls.isInstance(msg))
                    latch.countDown();
            }
        });

        long time = System.nanoTime();

        for (int i = 1; i <= SAMPLE_CNT; i++) {
            mgr0.sendToCustomTopic(grid(1).localNode(), topic, msg, GridIoPolicy.PUBLIC_POOL);

            if (i % 500 == 0)
                info("Sent messages count: " + i);
        }

        assert latch.await(3, SECONDS);

        time = System.nanoTime() - time;

        info(">>>");
        info(">>> send() time (ms): " + MILLISECONDS.convert(time, NANOSECONDS));
        info(">>>");
    }

    /** */
    private static class TestMessage implements Message {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!writer.writeHeader(directType(), (byte)0))
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }
    }

    /** */
    private static class TestOverByteIdMessage implements Message {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!writer.writeHeader(directType(), (byte)0))
                return false;

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onAckReceived() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
            return DIRECT_TYPE_OVER_BYTE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 0;
        }
    }

    /** */
    public static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_PLUGIN";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(MessageFactory.class, new MessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    factory.register(DIRECT_TYPE, TestMessage::new);
                    factory.register(DIRECT_TYPE_OVER_BYTE, TestOverByteIdMessage::new);
                }
            });
        }
    }
}
