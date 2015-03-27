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

package org.apache.ignite.messaging;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.config.*;
import org.apache.ignite.testframework.junits.common.*;
import org.junit.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.testframework.GridTestUtils.*;

/**
 * Various tests for Messaging public API.
 */
public class GridMessagingSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String MSG_1 = "MSG-1";

    /** */
    private static final String MSG_2 = "MSG-2";

    /** */
    private static final String MSG_3 = "MSG-3";

    /** */
    private static final String S_TOPIC_1 = "TOPIC-1";

    /** */
    private static final String S_TOPIC_2 = "TOPIC-2";

    /** */
    private static final Integer I_TOPIC_1 = 1;

    /** */
    private static final Integer I_TOPIC_2 = 2;

    /** */
    public static final String EXT_RESOURCE_CLS_NAME = "org.apache.ignite.tests.p2p.TestUserResource";

    /** Shared IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * A test message topic.
     */
    private enum TestTopic {
        /** */
        TOPIC_1,

        /** */
        TOPIC_2
    }

    /**
     * A test message with a hack for delay
     * emulation.
     */
    private static class TestMessage implements Externalizable {
        /** */
        private Object body;

        /** */
        private long delayMs;

        /**
         * No-arg constructor for {@link Externalizable}.
         */
        public TestMessage() {
            // No-op.
        }

        /**
         * @param body Message body.
         */
        TestMessage(Object body) {
            this.body = body;
        }

        /**
         * @param body Message body.
         * @param delayMs Message send delay in milliseconds.
         */
        TestMessage(Object body, long delayMs) {
            this.body = body;
            this.delayMs = delayMs;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestMessage [body=" + body + "]";
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return body.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestMessage && body.equals(((TestMessage)obj).body);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }

            out.writeObject(body);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            body = in.readObject();
        }
    }

    /** */
    protected Ignite ignite1;

    /** */
    protected Ignite ignite2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ignite1 = null;
        ignite2 = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((OptimizedMarshaller)cfg.getMarshaller()).setRequireSerializable(false);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /**
     * Tests simple message sending-receiving.
     *
     * @throws Exception If error occurs.
     */
    public void testSendReceiveMessage() throws Exception {
        final Collection<Object> rcvMsgs = new GridConcurrentHashSet<>();

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(3);

        ignite1.message().localListen(null, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                    if (!nodeId.equals(ignite2.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup rNode1 = ignite2.cluster().forRemotes();

        message(rNode1).send(null, MSG_1);
        message(rNode1).send(null, MSG_2);
        message(rNode1).send(null, MSG_3);

        assertTrue(rcvLatch.await(3, TimeUnit.SECONDS));

        assertFalse(error.get());

        assertTrue(rcvMsgs.contains(MSG_1));
        assertTrue(rcvMsgs.contains(MSG_2));
        assertTrue(rcvMsgs.contains(MSG_3));
    }

    /**
     * @throws Exception If error occurs.
     */
    @SuppressWarnings("TooBroadScope")
    public void testStopLocalListen() throws Exception {
        final AtomicInteger msgCnt1 = new AtomicInteger();

        final AtomicInteger msgCnt2 = new AtomicInteger();

        final AtomicInteger msgCnt3 = new AtomicInteger();

        P2<UUID, Object> lsnr1 = new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                log.info("Listener1 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt1.incrementAndGet();

                return true;
            }
        };

        P2<UUID, Object> lsnr2 = new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                log.info("Listener2 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt2.incrementAndGet();

                return true;
            }
        };

        P2<UUID, Object> lsnr3 = new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                log.info("Listener3 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt3.incrementAndGet();

                return true;
            }
        };

        final String topic1 = null;
        final String topic2 = "top1";
        final String topic3 = "top3";

        ignite1.message().localListen(topic1, lsnr1);
        ignite1.message().localListen(topic2, lsnr2);
        ignite1.message().localListen(topic3, lsnr3);

        ClusterGroup rNode1 = ignite2.cluster().forRemotes();

        message(rNode1).send(topic1, "msg1-1");
        message(rNode1).send(topic2, "msg1-2");
        message(rNode1).send(topic3, "msg1-3");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt1.get() > 0 && msgCnt2.get() > 0 && msgCnt3.get() > 0;
            }
        }, 5000);

        assertEquals(1, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(1, msgCnt3.get());

        ignite1.message().stopLocalListen(topic2, lsnr2);

        message(rNode1).send(topic1, "msg2-1");
        message(rNode1).send(topic2, "msg2-2");
        message(rNode1).send(topic3, "msg2-3");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt1.get() > 1 && msgCnt3.get() > 1;
            }
        }, 5000);

        assertEquals(2, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(2, msgCnt3.get());

        ignite1.message().stopLocalListen(topic2, lsnr1); // Try to use wrong topic for lsnr1 removing.

        message(rNode1).send(topic1, "msg3-1");
        message(rNode1).send(topic2, "msg3-2");
        message(rNode1).send(topic3, "msg3-3");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt1.get() > 2 && msgCnt3.get() > 2;
            }
        }, 5000);

        assertEquals(3, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(3, msgCnt3.get());

        ignite1.message().stopLocalListen(topic1, lsnr1);
        ignite1.message().stopLocalListen(topic3, lsnr3);

        message(rNode1).send(topic1, "msg4-1");
        message(rNode1).send(topic2, "msg4-2");
        message(rNode1).send(topic3, "msg4-3");

        U.sleep(1000);

        assertEquals(3, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(3, msgCnt3.get());
    }

    /**
     * Tests simple message sending-receiving with string topic.
     *
     * @throws Exception If error occurs.
     */
    public void testSendReceiveMessageWithStringTopic() throws Exception {
        final Collection<Object> rcvMsgs = new GridConcurrentHashSet<>();

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(3);

        ignite1.message().localListen(S_TOPIC_1, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + S_TOPIC_1 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_1.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + S_TOPIC_1);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite1.message().localListen(S_TOPIC_2, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + S_TOPIC_2 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_2.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + S_TOPIC_2);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite1.message().localListen(null, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=default]");

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_3.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: default");

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup rNode1 = ignite1.cluster().forLocal();

        message(rNode1).send(S_TOPIC_1, MSG_1);
        message(rNode1).send(S_TOPIC_2, MSG_2);
        message(rNode1).send(null, MSG_3);

        assertTrue(rcvLatch.await(3, TimeUnit.SECONDS));

        assertFalse(error.get());

        assertTrue(rcvMsgs.contains(MSG_1));
        assertTrue(rcvMsgs.contains(MSG_2));
        assertTrue(rcvMsgs.contains(MSG_3));
    }

    /**
     * Tests simple message sending-receiving with enumerated topic.
     *
     * @throws Exception If error occurs.
     */
    public void testSendReceiveMessageWithEnumTopic() throws Exception {
        final Collection<Object> rcvMsgs = new GridConcurrentHashSet<>();

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(3);

        ignite1.message().localListen(TestTopic.TOPIC_1, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + TestTopic.TOPIC_1 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_1.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + TestTopic.TOPIC_1);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite1.message().localListen(TestTopic.TOPIC_2, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + TestTopic.TOPIC_2 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_2.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + TestTopic.TOPIC_2);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite1.message().localListen(null, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=default]");

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_3.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: default");

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup rNode1 = ignite1.cluster().forLocal();

        message(rNode1).send(TestTopic.TOPIC_1, MSG_1);
        message(rNode1).send(TestTopic.TOPIC_2, MSG_2);
        message(rNode1).send(null, MSG_3);

        assertTrue(rcvLatch.await(3, TimeUnit.SECONDS));

        assertFalse(error.get());

        assertTrue(rcvMsgs.contains(MSG_1));
        assertTrue(rcvMsgs.contains(MSG_2));
        assertTrue(rcvMsgs.contains(MSG_3));
    }

    /**
     * Tests simple message sending-receiving with the use of
     * remoteListen() method.
     *
     * @throws Exception If error occurs.
     */
    public void testRemoteListen() throws Exception {
        final Collection<Object> rcvMsgs = new GridConcurrentHashSet<>();

        final CountDownLatch rcvLatch = new CountDownLatch(4);

        ignite2.message().remoteListen(null, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup prj2 = ignite1.cluster().forRemotes(); // Includes node from grid2.

        message(prj2).send(null, MSG_1);
        message(prj2).send(null, MSG_2);
        message(ignite2.cluster().forLocal()).send(null, MSG_3);

        assertFalse(rcvLatch.await(3, TimeUnit.SECONDS)); // We should get only 3 message.

        assertTrue(rcvMsgs.contains(MSG_1));
        assertTrue(rcvMsgs.contains(MSG_2));
        assertTrue(rcvMsgs.contains(MSG_3));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testStopRemoteListen() throws Exception {
        final AtomicInteger msgCnt1 = new AtomicInteger();

        final AtomicInteger msgCnt2 = new AtomicInteger();

        final AtomicInteger msgCnt3 = new AtomicInteger();

        final String topic1 = null;
        final String topic2 = "top2";
        final String topic3 = "top3";

        UUID id1 = ignite2.message().remoteListen(topic1, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(Thread.currentThread().getName() + " Listener1 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt1.incrementAndGet();

                return true;
            }
        });

        UUID id2 = ignite2.message().remoteListen(topic2, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(Thread.currentThread().getName() + " Listener2 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt2.incrementAndGet();

                return true;
            }
        });

        UUID id3 = ignite2.message().remoteListen(topic3, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(Thread.currentThread().getName() + " Listener3 received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt3.incrementAndGet();

                return true;
            }
        });

        message(ignite1.cluster().forRemotes()).send(topic1, "msg1-1");
        message(ignite1.cluster().forRemotes()).send(topic2, "msg1-2");
        message(ignite1.cluster().forRemotes()).send(topic3, "msg1-3");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt1.get() > 0 && msgCnt2.get() > 0 && msgCnt3.get() > 0;
            }
        }, 5000);

        assertEquals(1, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(1, msgCnt3.get());

        ignite2.message().stopRemoteListen(id2);

        message(ignite1.cluster().forRemotes()).send(topic1, "msg2-1");
        message(ignite1.cluster().forRemotes()).send(topic2, "msg2-2");
        message(ignite1.cluster().forRemotes()).send(topic3, "msg2-3");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt1.get() > 1 && msgCnt3.get() > 1;
            }
        }, 5000);

        assertEquals(2, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(2, msgCnt3.get());

        ignite2.message().stopRemoteListen(id2); // Try remove one more time.

        ignite2.message().stopRemoteListen(id1);
        ignite2.message().stopRemoteListen(id3);

        message(ignite1.cluster().forRemotes()).send(topic1, "msg3-1");
        message(ignite1.cluster().forRemotes()).send(topic2, "msg3-2");
        message(ignite1.cluster().forRemotes()).send(topic3, "msg3-3");

        U.sleep(1000);

        assertEquals(2, msgCnt1.get());
        assertEquals(1, msgCnt2.get());
        assertEquals(2, msgCnt3.get());
    }

    /**
     * Tests simple message sending-receiving with the use of
     * remoteListen() method.
     *
     * @throws Exception If error occurs.
     */
    public void testRemoteListenOrderedMessages() throws Exception {
        List<TestMessage> msgs = Arrays.asList(
            new TestMessage(MSG_1),
            new TestMessage(MSG_2, 3000),
            new TestMessage(MSG_3));

        final Collection<Object> rcvMsgs = new ConcurrentLinkedDeque<>();

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(3);

        ignite2.message().remoteListen(S_TOPIC_1, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup prj2 = ignite1.cluster().forRemotes(); // Includes node from grid2.

        for (TestMessage msg : msgs)
            message(prj2).sendOrdered(S_TOPIC_1, msg, 15000);

        assertTrue(rcvLatch.await(6, TimeUnit.SECONDS));

        assertFalse(error.get());

        //noinspection AssertEqualsBetweenInconvertibleTypes
        assertEquals(msgs, Arrays.asList(rcvMsgs.toArray()));
    }

    /**
     * Tests simple message sending-receiving with the use of
     * remoteListen() method and topics.
     *
     * @throws Exception If error occurs.
     */
    public void testRemoteListenWithIntTopic() throws Exception {
        final Collection<Object> rcvMsgs = new GridConcurrentHashSet<>();

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(3);

        ignite2.message().remoteListen(I_TOPIC_1, new P2<UUID, Object>() {
            @IgniteInstanceResource
            private transient Ignite g;

            @Override public boolean apply(UUID nodeId, Object msg) {
                assertEquals(ignite2, g);

                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + I_TOPIC_1 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_1.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + I_TOPIC_1);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite2.message().remoteListen(I_TOPIC_2, new P2<UUID, Object>() {
            @IgniteInstanceResource
            private transient Ignite g;

            @Override public boolean apply(UUID nodeId, Object msg) {
                assertEquals(ignite2, g);

                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=" + I_TOPIC_2 + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_2.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: " + I_TOPIC_2);

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ignite2.message().remoteListen(null, new P2<UUID, Object>() {
            @IgniteInstanceResource
            private transient Ignite g;

            @Override public boolean apply(UUID nodeId, Object msg) {
                assertEquals(ignite2, g);

                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId +
                        ", topic=default]");

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    if (!MSG_3.equals(msg)) {
                        log.error("Unexpected message " + msg + " for topic: default");

                        error.set(true);

                        return false;
                    }

                    rcvMsgs.add(msg);

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        ClusterGroup prj2 = ignite1.cluster().forRemotes(); // Includes node from grid2.

        message(prj2).send(I_TOPIC_1, MSG_1);
        message(prj2).send(I_TOPIC_2, MSG_2);
        message(prj2).send(null, MSG_3);

        assertTrue(rcvLatch.await(3, TimeUnit.SECONDS));

        assertFalse(error.get());

        assertTrue(rcvMsgs.contains(MSG_1));
        assertTrue(rcvMsgs.contains(MSG_2));
        assertTrue(rcvMsgs.contains(MSG_3));
    }

    /**
     * Checks, if it is OK to send the message, loaded with external
     * class loader.
     *
     * @throws Exception If error occurs.
     */
    public void testSendMessageWithExternalClassLoader() throws Exception {
        URL[] urls = new URL[] { new URL(GridTestProperties.getProperty("p2p.uri.cls")) };

        ClassLoader extLdr = new URLClassLoader(urls);

        Class rcCls = extLdr.loadClass(EXT_RESOURCE_CLS_NAME);

        final AtomicBoolean error = new AtomicBoolean(false); //to make it modifiable

        final CountDownLatch rcvLatch = new CountDownLatch(1);

        ignite2.message().remoteListen(S_TOPIC_1, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                try {
                    log.info("Received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                    if (!nodeId.equals(ignite1.cluster().localNode().id())) {
                        log.error("Unexpected sender node: " + nodeId);

                        error.set(true);

                        return false;
                    }

                    return true;
                }
                finally {
                    rcvLatch.countDown();
                }
            }
        });

        message(ignite1.cluster().forRemotes()).send(S_TOPIC_1, Collections.singleton(rcCls.newInstance()));

        assertTrue(rcvLatch.await(3, TimeUnit.SECONDS));

        assertFalse(error.get());
    }

    /**
     * Test case for {@code null} messages.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testNullMessages() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.message().send(null, null);

                return null;
            }
        }, IllegalArgumentException.class, "Ouch! Argument is invalid: msgs cannot be null or empty");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.message().send(null, Collections.emptyList());

                return null;
            }
        }, IllegalArgumentException.class, "Ouch! Argument is invalid: msgs cannot be null or empty");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.message().send(null, (Object)null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: msg");

        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                ignite1.message().send(null, Arrays.asList(null, new Object()));

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: msg");
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsync() throws Exception {
        final AtomicInteger msgCnt = new AtomicInteger();

        assertFalse(ignite2.message().isAsync());

        final IgniteMessaging msg = ignite2.message().withAsync();

        assertTrue(msg.isAsync());

        assertFalse(ignite2.message().isAsync());

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                msg.future();

                return null;
            }
        }, IllegalStateException.class, null);

        final String topic = "topic";

        UUID id = msg.remoteListen(topic, new P2<UUID, Object>() {
            @Override public boolean apply(UUID nodeId, Object msg) {
                System.out.println(Thread.currentThread().getName() +
                    " Listener received new message [msg=" + msg + ", senderNodeId=" + nodeId + ']');

                msgCnt.incrementAndGet();

                return true;
            }
        });

        Assert.assertNull(id);

        IgniteFuture<UUID> fut = msg.future();

        Assert.assertNotNull(fut);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                msg.future();

                return null;
            }
        }, IllegalStateException.class, null);

        id = fut.get();

        Assert.assertNotNull(id);

        message(ignite1.cluster().forRemotes()).send(topic, "msg1");

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return msgCnt.get() > 0;
            }
        }, 5000);

        assertEquals(1, msgCnt.get());

        msg.stopRemoteListen(id);

        IgniteFuture<?> stopFut = msg.future();

        Assert.assertNotNull(stopFut);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                msg.future();

                return null;
            }
        }, IllegalStateException.class, null);

        stopFut.get();

        message(ignite1.cluster().forRemotes()).send(topic, "msg2");

        U.sleep(1000);

        assertEquals(1, msgCnt.get());
    }
}
