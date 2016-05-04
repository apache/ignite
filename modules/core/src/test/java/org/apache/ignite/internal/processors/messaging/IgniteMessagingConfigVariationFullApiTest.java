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

package org.apache.ignite.internal.processors.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * The test checks process messaging.
 */
public class IgniteMessagingConfigVariationFullApiTest extends IgniteConfigVariationsAbstractTest {

    private static final String LATCH_NAME = "latchName";
    private static final String MESSAGE_TOPIC = "topic";

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        return getTestGridName(CLIENT_NODE_IDX).equals(testGridName)
            || getTestGridName(3).equals(testGridName)
            || getTestGridName(5).equals(testGridName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalServer() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                localServerInternal();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testLocalListener() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                localListenerInternal();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerClientMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                serverClientMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientClientMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                clientClientMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientServerMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                clientServerMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollectionMessage() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                collectionMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testOrderedMessage() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                orderedMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientServerOrderedMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                clientServerOrderedMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientClientOrderedMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                clientClientOrderedMessage();
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerClientOrderedMessage() throws Exception {
        if (!testsCfg.withClients())
            return;

        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                serverClientOrderedMessage();
            }
        });
    }

    /**
     * Single server test.
     * @throws Exception If failed.
     */
    private void localServerInternal() throws Exception {
        int messages = 5;

        Ignite ignite = grid(SERVER_NODE_IDX);

        IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME, messages, true, true);

        ClusterGroup grp = grid(SERVER_NODE_IDX).cluster().forLocal();

        UUID opId = registerListener(grp);

        try {
            for (int i = 0; i < messages; i++)
                sendMessage(grp, value(i));

            assertTrue(latch.await(10, TimeUnit.SECONDS));

        }
        finally {
            ignite.message().stopRemoteListen(opId);
        }
    }

    /**
     * Single server test with local listener.
     */
    private void localListenerInternal() {
        int messages = 5;

        Ignite ignite = grid(SERVER_NODE_IDX);

        IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME, messages, true, true);

        ClusterGroup grp = grid(SERVER_NODE_IDX).cluster().forLocal();

        MessageListener c = new MessageListener(LATCH_NAME);

        try {
            ignite.message(grp).localListen("localListenerTopic", c);

            for (int i = 0; i < messages; i++)
                ignite.message(grp).send("localListenerTopic", value(i));

            assertTrue(latch.await(10, TimeUnit.SECONDS));

        }
        finally {
            ignite.message().stopLocalListen("localListenerTopic", c);
        }
    }

    /**
     * Server sends a message and client receives it.
     * @throws Exception If failed.
     */
    private void serverClientMessage() throws Exception {
        int messages = 5;

        Ignite ignite = grid(SERVER_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forClients();

        assert grp.nodes().size() > 0;

        registerListenerAndSendMessages(ignite, grp);
    }

    /**
     * Client sends a message and client receives it.
     * @throws Exception If failed.
     */
    private void clientClientMessage() throws Exception {
        Ignite ignite = grid(CLIENT_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forClients();

        assert grp.nodes().size() > 0;

        registerListenerAndSendMessages(ignite, grp);
    }

    /**
     * Client sends a message and client receives it.
     * @throws Exception If failed.
     */
    private void clientServerMessage() throws Exception {
        Ignite ignite = grid(CLIENT_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forServers();

        assert grp.nodes().size() > 0;

        registerListenerAndSendMessages(ignite, grp);
    }

    private void registerListenerAndSendMessages(Ignite ignite, ClusterGroup grp) throws Exception {
        int messages = 5;

        IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME, grp.nodes().size() * messages, true, true);

        UUID opId = registerListener(grp);
        try {
            for (int i = 0; i < messages; i++)
                sendMessage(grp, value(i));

            assertTrue(latch.await(10, TimeUnit.SECONDS));

        }
        finally {
            ignite.message().stopRemoteListen(opId);
        }
    }

    /**
     *
     */
    private void collectionMessage() {
        Ignite ignite = grid(SERVER_NODE_IDX);

        ClusterGroup grp = gridCount() > 1
            ? ignite.cluster().forRemotes()
            : ignite.cluster().forLocal();

        assert grp.nodes().size() > 0;

        int messages = 5;

        IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME,
            grp.nodes().size() * messages, true, true);

        UUID opId = ignite.message(grp).remoteListen(MESSAGE_TOPIC, new MessageListener(LATCH_NAME));

        try {
            List<Object> msgs = new ArrayList<>();
            for (int i = 0; i < messages; i++)
                msgs.add(value(i));

            ignite.message(grp).send(MESSAGE_TOPIC, msgs);

            assertTrue(latch.await(10, TimeUnit.SECONDS));

        } finally {
            ignite.message().stopRemoteListen(opId);
        }

    }


    /**
     *
     */
    private void orderedMessage() {
        Ignite ignite = grid(SERVER_NODE_IDX);

        ClusterGroup grp = gridCount() > 1
            ? ignite.cluster().forRemotes()
            : ignite.cluster().forLocal();

        assert grp.nodes().size() > 0;

        registerListenerAndSendOrderedMessages(ignite, grp);
    }

    /**
     *
     */
    private void clientServerOrderedMessage() {
        Ignite ignite = grid(CLIENT_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forServers();

        assert grp.nodes().size() > 0;

        registerListenerAndSendOrderedMessages(ignite, grp);
    }

    /**
     *
     */
    private void clientClientOrderedMessage() {
        Ignite ignite = grid(CLIENT_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forClients();

        assert grp.nodes().size() > 0;

        registerListenerAndSendOrderedMessages(ignite, grp);
    }

    /**
     *
     */
    private void serverClientOrderedMessage() {
        Ignite ignite = grid(SERVER_NODE_IDX);

        ClusterGroup grp = ignite.cluster().forClients();

        assert grp.nodes().size() > 0;

        registerListenerAndSendOrderedMessages(ignite, grp);
    }

    /**
     * @param ignite Ignite.
     * @param grp Cluster group.
     */
    private void registerListenerAndSendOrderedMessages(Ignite ignite, ClusterGroup grp) {
        int messages = 5;

        IgniteCountDownLatch latch = ignite.countDownLatch(LATCH_NAME,
            grp.nodes().size() * messages, true, true);

        UUID opId = ignite.message(grp).remoteListen(MESSAGE_TOPIC, new OrderedMessageListener(LATCH_NAME));

        try {
            for (int i=0; i < messages; i++)
                ignite.message(grp).sendOrdered(MESSAGE_TOPIC, value(i), 2000);

            assertTrue(latch.await(10, TimeUnit.SECONDS));

        } finally {
            ignite.message().stopRemoteListen(opId);
        }

    }

    /**
     * @param grp Cluster group.
     * @param msg Message.
     */
    private void sendMessage(ClusterGroup grp, Object msg) {
        Ignite ignite = grid(SERVER_NODE_IDX);

        ignite.message(grp).send(MESSAGE_TOPIC, msg);
    }

    /**
     * @param grp Cluster group.
     * @return Message listener uuid.
     * @throws Exception If failed.
     */
    private UUID registerListener(ClusterGroup grp) throws Exception {
        Ignite ignite = grid(SERVER_NODE_IDX);

        IgniteBiPredicate<UUID,Object> listener = new MessageListener(LATCH_NAME);

        return ignite.message(grp).remoteListen(MESSAGE_TOPIC, listener);
    }

    /**
     * Ignite predicate.
     */
    private static class MessageListener implements IgniteBiPredicate<UUID,Object> {

        /**
         * Ignite
         */
        @IgniteInstanceResource
        transient Ignite ignite;

        /**
         * Ignite latch.
         */
        protected String latchName;

        /**
         * @param latchName Ignite latch name.
         */
        public MessageListener(String latchName) {
            this.latchName = latchName;
        }

        /**
         * Default constructor.
         */
        public MessageListener() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, Object msg) {
            IgniteCountDownLatch latch = ignite.countDownLatch(latchName, 0, false, false);
            latch.countDown();

            return true;
        }
    }

    /**
     * Ignite order predicate.
     */
    private static class OrderedMessageListener implements IgniteBiPredicate<UUID,TestObject> {

        /**
         * Ignite
         */
        @IgniteInstanceResource
        transient Ignite ignite;

        /**
         * Ignite latch name.
         */
        private String latchName;

        /**
         * Counter.
         */
        private AtomicInteger cntr;

        /**
         * @param latchName Ignite latch name.
         */
        OrderedMessageListener(String latchName) {
            this.latchName = latchName;

            cntr = new AtomicInteger(0);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, TestObject msg) {
            assertEquals(cntr.getAndIncrement(), msg.value());

            IgniteCountDownLatch latch = ignite.countDownLatch(latchName, 0, false, false);
            latch.countDown();

            return true;
        }
    }
}
