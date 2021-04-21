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
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Messaging test.
 */
public class GridCacheMessageSelfTest extends GridCommonAbstractTest {
    /** Sample count. */
    private static final int SAMPLE_CNT = 1;

    /** Latch on failure processor. */
    private static CountDownLatch failureLatch;

    /** */
    public static final String TEST_BODY = "Test body";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setIncludeEventTypes((int[])null);

        cfg.setFailureHandler(new TestFailureHandler());

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setBackups(0);

        cfg.setCacheConfiguration(ccfg);

        cfg.setPluginProviders(new TestPluginProvider());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        failureLatch = new CountDownLatch(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testSendBadMessage() throws Exception {
        try {
            startGrids(2);

            IgniteEx ignite0 = grid(0);
            IgniteEx ignite1 = grid(1);

            ignite0.context().cache().context().io().addCacheHandler(
                0, TestBadMessage.class, new CI2<UUID, GridCacheMessage>() {
                @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                    throw new RuntimeException("Test bad message exception");
                }
            });

            ignite1.context().cache().context().io().addCacheHandler(
                0, TestBadMessage.class, new CI2<UUID, GridCacheMessage>() {
                    @Override public void apply(UUID nodeId, GridCacheMessage msg) {
                        throw new RuntimeException("Test bad message exception");
                    }
                });

            ignite0.context().cache().context().io().send(
                ignite1.localNode().id(), new TestBadMessage(), (byte)2);

            boolean res = failureLatch.await(5, TimeUnit.SECONDS);

            assertTrue(res);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void doSend() throws Exception {
        GridIoManager mgr0 = grid(0).context().io();
        GridIoManager mgr1 = grid(1).context().io();

        String topic = "test-topic";

        final CountDownLatch latch = new CountDownLatch(SAMPLE_CNT);

        mgr1.addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                try {
                    latch.countDown();

                    Collection<TestMessage1> messages = ((TestMessage) msg).entries();

                    assertEquals(10, messages.size());

                    int cnt = 0;

                    for (TestMessage1 msg1 : messages) {
                        assertTrue(msg1.body().contains(TEST_BODY));

                        int i = Integer.parseInt(msg1.body().substring(TEST_BODY.length() + 1));

                        assertEquals(cnt, i);

                        TestMessage2 msg2 = (TestMessage2) msg1.message();

                        assertEquals(TEST_BODY + "_" + i + "_2", msg2.body());

                        assertEquals(grid(0).localNode().id(), msg2.nodeId());

                        assertEquals(i, msg2.id());

                        GridTestMessage msg3 = (GridTestMessage) msg2.message();

                        assertEquals(cnt, msg3.getMsgId());

                        assertEquals(grid(1).localNode().id(), msg3.getSourceNodeId());

                        cnt++;
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

        mgr0.sendToCustomTopic(grid(1).localNode(), topic, msg, GridIoPolicy.PUBLIC_POOL);

        assert latch.await(3, SECONDS);
    }

    /** */
    private static class TestMessage extends GridCacheMessage {
        /** */
        public static final short DIRECT_TYPE = 202;

        /** */
        @GridDirectCollection(TestMessage1.class)
        private Collection<TestMessage1> entries = new ArrayList<>();

        /**
         * @param entry Entry.
         */
        public void add(TestMessage1 entry) {
            entries.add(entry);
        }

        /** {@inheritDoc} */
        @Override public int handlerId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheGroupMessage() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
        }

        /**
         * @return COllection of test messages.
         */
        public Collection<TestMessage1> entries() {
            return entries;
        }

        /** {@inheritDoc} */
        @Override public short directType() {
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
        public static final short DIRECT_TYPE = 203;

        /** Body. */
        private String body;

        /** */
        private Message msg;

        /**
         * @param msg Message.
         * @param body Message body.
         */
        public void init(Message msg, String body) {
            this.msg = msg;
            this.body = body;
        }

        /** {@inheritDoc} */
        @Override public int handlerId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheGroupMessage() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
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
        @Override public short directType() {
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
        public static final short DIRECT_TYPE = 201;

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

        /** {@inheritDoc} */
        @Override public int handlerId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheGroupMessage() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
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
        @Override public short directType() {
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

    /**
     * Test message class.
     */
    static class TestBadMessage extends GridCacheMessage {
        /** */
        public static final short DIRECT_TYPE = 204;

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

        /** {@inheritDoc} */
        @Override public int handlerId() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public boolean cacheGroupMessage() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean addDeploymentInfo() {
            return false;
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
        @Override public short directType() {
            return DIRECT_TYPE;
        }

        /** {@inheritDoc} */
        @Override public byte fieldsCount() {
            return 7;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            throw new RuntimeException("Exception while log message");
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

    /** */
    private static class TestFailureHandler extends AbstractFailureHandler {
        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            failureLatch.countDown();

            return false;
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
                    factory.register(TestMessage.DIRECT_TYPE, TestMessage::new);
                    factory.register(GridTestMessage.DIRECT_TYPE, GridTestMessage::new);
                    factory.register(TestMessage1.DIRECT_TYPE, TestMessage1::new);
                    factory.register(TestMessage2.DIRECT_TYPE, TestMessage2::new);
                    factory.register(TestBadMessage.DIRECT_TYPE, TestBadMessage::new);
                }
            });
        }
    }
}
