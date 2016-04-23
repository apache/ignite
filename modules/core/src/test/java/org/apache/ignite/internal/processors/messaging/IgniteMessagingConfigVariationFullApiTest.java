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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The test checks process messaging.
 */
public class IgniteMessagingConfigVariationFullApiTest extends IgniteConfigVariationsAbstractTest {

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        return getTestGridName(CLIENT_NODE_IDX).equals(testGridName)
            || getTestGridName(3).equals(testGridName)
            || getTestGridName(5).equals(testGridName);
    }

    /**
     * @param grp
     * @param message
     */
    private void sentMessage(ClusterGroup grp, Object message) {
        Ignite ignite = grid(SERVER_NODE_IDX);
        if (ignite == null)
            return;
        ignite.message(grp).send("topic", message);
    }

    /**
     * @param grp Cluster group.
     * @param closure Ignite predicate.
     */
    private UUID registerListener(ClusterGroup grp, IgniteBiPredicate<UUID, Object> closure) {
        Ignite ignite = grid(SERVER_NODE_IDX);
        if (ignite == null)
            return null;
        return ignite.message(grp).remoteListen("topic", closure);
    }

    /**
     * Test method.
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
     * Test method.
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
     * Test method.
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
     * Test method.
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
     * Test method.
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

    public void testOrderMessage() throws Exception {
        runInAllDataModes(new TestRunnable() {
            @Override public void run() throws Exception {
                orderMessage();
            }
        });
    }

    /**
     * Single server test.
     */
    private void localServerInternal() {
        int messages = 5;
        Ignite ignite = grid(SERVER_NODE_IDX);
        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", messages, true, true);
        ClusterGroup grp = grid(SERVER_NODE_IDX).cluster().forLocal();

        UUID opId = registerListener(grp, new IgnitePredicate(latch));

        for (int i=0; i<messages; i++)
            sentMessage(grp, value(i));

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        ignite.message().stopRemoteListen(opId);
    }

    /**
     * Single server test with local listener.
     */
    private void localListenerInternal() {
        int messages = 5;
        Ignite ignite = grid(SERVER_NODE_IDX);
        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", messages, true, true);
        ClusterGroup grp = grid(SERVER_NODE_IDX).cluster().forLocal();

        IgnitePredicate closure = new IgnitePredicate(latch);
        ignite.message(grp).localListen("localListenerTopic", closure);

        for (int i=0; i<messages; i++)
            ignite.message(grp).send("localListenerTopic", value(i));

        assertTrue(latch.await(10, TimeUnit.SECONDS));

//        ignite.message().stopLocalListen("localListenerTopic", closure);
    }

    /**
     * Server sends a message and client receives it.
     */
    private void serverClientMessage() {
        int messages = 5;
        Ignite ignite = grid(SERVER_NODE_IDX);
        ClusterGroup grp = ignite.cluster().forClients();
        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", grp.nodes().size() * messages, true, true);

        UUID opId = registerListener(grp, new IgnitePredicate(latch));

        for (int i=0; i<messages; i++)
            sentMessage(grp, value(i));

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        ignite.message().stopRemoteListen(opId);
    }

    /**
     * Client sends a message and client receives it.
     */
    private void clientClientMessage() {
        int messages = 5;
        Ignite ignite = grid(CLIENT_NODE_IDX);
        ClusterGroup grp = ignite.cluster().forClients();
        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", grp.nodes().size() * messages, true, true);

        UUID opId = registerListener(grp, new IgnitePredicate(latch));

        for (int i=0; i<messages; i++)
            sentMessage(grp, value(i));

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        ignite.message().stopRemoteListen(opId);
    }

    /**
     * Client sends a message and client receives it.
     */
    private void clientServerMessage() {
        int messages = 5;
        Ignite ignite = grid(CLIENT_NODE_IDX);
        ClusterGroup grp = ignite.cluster().forServers();
        IgniteCountDownLatch latch = ignite.countDownLatch("latchName", grp.nodes().size() * messages, true, true);

        UUID opId = registerListener(grp, new IgnitePredicate(latch));

        for (int i=0; i<messages; i++)
            sentMessage(grp, value(i));

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        ignite.message().stopRemoteListen(opId);
    }

    private void orderMessage() {
        int messages = 5;
        Ignite ignite = grid(SERVER_NODE_IDX);
        ClusterGroup grp = gridCount() > 1
            ? ignite.cluster().forRemotes()
            : ignite.cluster().forLocal();
        IgniteCountDownLatch latch = ignite.countDownLatch("orderedTestLatch",
            grp.nodes().size() * messages, true, true);

        UUID opId = ignite.message(grp).remoteListen("ordered", new IgniteOrderPredicate(latch));

        for (int i=0; i<messages; i++)
            ignite.message(grp).sendOrdered("ordered", value(i), 2000);

        assertTrue(latch.await(10, TimeUnit.SECONDS));

        ignite.message().stopRemoteListen(opId);

    }

    /**
     * Ignite predicate.
     */
    private static class IgnitePredicate implements IgniteBiPredicate<UUID,Object>, Serializable {

        /**
         * Ignite latch.
         */
        private IgniteCountDownLatch latch;

        /**
         * @param latch Ignite latch.
         */
        public IgnitePredicate(IgniteCountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, Object msg) {
            latch.countDown();
            return true;
        }
    }

    /**
     * Ignite order predicate.
     */
    private static class IgniteOrderPredicate implements IgniteBiPredicate<UUID,TestObject>, Serializable {

        /**
         * Ignite latch.
         */
        private IgniteCountDownLatch latch;

        /**
         * Counter.
         */
        private AtomicInteger counter;

        /**
         * @param latch Ignite latch.
         */
        public IgniteOrderPredicate(IgniteCountDownLatch latch) {
            this.latch = latch;
            counter = new AtomicInteger(0);
        }

        /** {@inheritDoc} */
        @Override public boolean apply(UUID nodeId, TestObject msg) {
            assertEquals(counter.getAndIncrement(), msg.value());
            latch.countDown();
            return true;
        }
    }

}
