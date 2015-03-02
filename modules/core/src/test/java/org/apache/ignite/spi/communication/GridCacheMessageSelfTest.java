package org.apache.ignite.spi.communication;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.testframework.junits.common.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Messaging test.
 */
public class GridCacheMessageSelfTest extends GridCommonAbstractTest {
    /** Sample count. */
    private static final int SAMPLE_CNT = 1;

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
    }

    public static final String TEST_BODY = "Test body";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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

                        GridTestMessage msg2 = (GridTestMessage) msg1.message();

                        assertEquals(count, msg2.getMsgId());

                        assertEquals(grid(1).localNode().id(), msg2.getSourceNodeId());

                        count++;
                    }
                }
                catch (Exception e) {
                    fail("Exception " + e.getStackTrace());
                }
            }
        });

        TestMessage msg = new TestMessage();

        for (int i = 0; i < 10; i++) {
            TestMessage1 mes1 = new TestMessage1();

            mes1.init(new GridTestMessage(grid(1).localNode().id(), i, 0), TEST_BODY + "_" + i);

            msg.add(mes1);
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
}
