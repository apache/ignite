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

package org.apache.ignite.spi.communication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Messaging test.
 */
public class GridCacheMessageSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Sample count. */
    private static final int SAMPLE_CNT = 1;

    /** */
    public static final String TEST_BODY = "Test body";

    /**
     *
     */
    static {
        GridIoMessageFactory.registerCustom(TestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new TestMessage();
            }
        });

        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new GridTestMessage();
            }
        });

        GridIoMessageFactory.registerCustom(TestMessage1.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new TestMessage1();
            }
        });

        GridIoMessageFactory.registerCustom(TestMessage2.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new TestMessage2();
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setIncludeEventTypes((int[])null);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSendMessage() throws Exception {
        try {
            startGridsMultiThreaded(2);

            doSend();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doSend() throws Exception {
        GridIoManager mgr0 = ((IgniteKernal)grid(0)).context().io();
        GridIoManager mgr1 = ((IgniteKernal)grid(1)).context().io();

        String topic = "test-topic";

        final CountDownLatch latch = new CountDownLatch(SAMPLE_CNT);

        mgr1.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                try {
                    latch.countDown();

                    Collection<TestMessage1> messages = ((TestMessage) msg).entries();

                    assertEquals(10, messages.size());

                    int count = 0;

                    for (TestMessage1 msg1 : messages) {
                        assertTrue(msg1.body().contains(TEST_BODY));

                        int i = Integer.parseInt(msg1.body().substring(TEST_BODY.length() + 1));

                        assertEquals(count, i);

                        TestMessage2 msg2 = (TestMessage2) msg1.message();

                        assertEquals(TEST_BODY + "_" + i + "_2", msg2.body());

                        assertEquals(grid(0).localNode().id(), msg2.nodeId());

                        assertEquals(i, msg2.id());

                        GridTestMessage msg3 = (GridTestMessage) msg2.message();

                        assertEquals(count, msg3.getMsgId());

                        assertEquals(grid(1).localNode().id(), msg3.getSourceNodeId());

                        count++;
                    }
                }
                catch (Exception e) {
                    fail("Exception " + e.getMessage());
                }
            }
        });

        TestMessage msg = new TestMessage();

        for (int i = 0; i < 10; i++) {
            TestMessage2 mes1 = new TestMessage2();

            mes1.init(new GridTestMessage(grid(1).localNode().id(), i, 0),
                grid(0).localNode().id(), i, TEST_BODY + "_" + i + "_2");

            TestMessage1 mes2 = new TestMessage1();

            mes2.init(mes1, TEST_BODY + "_" + i);

            msg.add(mes2);
        }

        mgr0.send(grid(1).localNode(), topic, msg, GridIoPolicy.PUBLIC_POOL);

        assert latch.await(3, SECONDS);
    }

    /** */
    private static class TestMessage extends GridCacheMessage {
        /** */
        public static final byte DIRECT_TYPE = (byte)202;

        /** */
        @GridDirectCollection(TestMessage1.class)
        private Collection<TestMessage1> entries = new ArrayList<>();

        /**
         * @param entry Entry.
         */
        public void add(TestMessage1 entry) {
            entries.add(entry);
        }

        /**
         * @return COllection of test messages.
         */
        public Collection<TestMessage1> entries() {
            return entries;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 4;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!super.writeTo(buf, writer))
                return false;

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(directType(), fieldsCount()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 3:
                    if (!writer.writeCollection("entries", entries, MessageCollectionItemType.MSG))
                        return false;

                    writer.incrementState();

            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            if (!super.readFrom(buf, reader))
                return false;

            switch (reader.state()) {
                case 3:
                    entries = reader.readCollection("entries", MessageCollectionItemType.MSG);

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

            }

            return true;
        }
    }

    /**
    * Test message class.
    */
    static class TestMessage1 extends GridCacheMessage {
        /** */
        public static final byte DIRECT_TYPE = (byte) 203;

        /** Body. */
        private String body;

        /** */
        private Message msg;

        /**
         * @param mes Message.
         */
        public void init(Message mes, String body) {
            this.msg = mes;

            this.body = body;
        }

        /**
         * @return Body.
         */
        public String body() {
            return body;
        }

        /**
         * @return Message.
         */
        public Message message() {
            return msg;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 5;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!super.writeTo(buf, writer))
                return false;

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(directType(), fieldsCount()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 3:
                    if (!writer.writeString("body", body))
                        return false;

                    writer.incrementState();

                case 4:
                    if (!writer.writeMessage("msg", msg))
                        return false;

                    writer.incrementState();

            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            if (!super.readFrom(buf, reader))
                return false;

            switch (reader.state()) {
                case 3:
                    body = reader.readString("body");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 4:
                    msg = reader.readMessage("msg");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

            }

            return true;
        }
    }

    /**
     * Test message class.
     */
    static class TestMessage2 extends GridCacheMessage {
        /** */
        public static final byte DIRECT_TYPE = (byte) 205;

        /** Node id. */
        private UUID nodeId;

        /** Integer field. */
        private int id;

        /** Body. */
        private String body;

        /** */
        private Message msg;

        /**
         * @param mes Message.
         */
        public void init(Message mes, UUID nodeId, int id, String body) {
            this.nodeId = nodeId;
            this.id = id;
            this.msg = mes;
            this.body = body;
        }

        /**
         * @return Body.
         */
        public String body() {
            return body;
        }

        /**
         * @return Message.
         */
        public Message message() {
            return msg;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Id.
         */
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public byte directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 7;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            writer.setBuffer(buf);

            if (!super.writeTo(buf, writer))
                return false;

            if (!writer.isHeaderWritten()) {
                if (!writer.writeHeader(directType(), fieldsCount()))
                    return false;

                writer.onHeaderWritten();
            }

            switch (writer.state()) {
                case 3:
                    if (!writer.writeUuid("nodeId", nodeId))
                        return false;

                    writer.incrementState();

                case 4:
                    if (!writer.writeInt("id", id))
                        return false;

                    writer.incrementState();

                case 5:
                    if (!writer.writeString("body", body))
                        return false;

                    writer.incrementState();

                case 6:
                    if (!writer.writeMessage("msg", msg))
                        return false;

                    writer.incrementState();

            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
            reader.setBuffer(buf);

            if (!reader.beforeMessageRead())
                return false;

            if (!super.readFrom(buf, reader))
                return false;

            switch (reader.state()) {
                case 3:
                    nodeId = reader.readUuid("nodeId");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 4:
                    id = reader.readInt("id");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 5:
                    body = reader.readString("body");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                case 6:
                    msg = reader.readMessage("msg");

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

            }

            return true;
        }
    }
}