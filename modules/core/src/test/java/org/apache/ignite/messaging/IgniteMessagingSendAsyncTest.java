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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class IgniteMessagingSendAsyncTest extends GridCommonAbstractTest {
    /**
     * Ignite instance for test.
     */
    private Ignite ignite1;

    private Ignite ignite2;

    /**
     * Topic name.
     */
    private final String TOPIC = "topic";

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test for check, that if use default mode, local listeners execute
     * in the same thread. 1 node in topology.
     */
    public void testSendDefaultMode() throws InterruptedException {
        stopGrid(2);

        final String msgStr = "message";

        send(ignite1.message(), msgStr, new ProcedureApply() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertEquals(Thread.currentThread(), thread);
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * Test for check, that if use async mode, local listeners execute
     * in another thread(through pool). 1 node in topology.
     */
    public void testSendAsyncMode() throws InterruptedException {
        stopGrid(2);

        final String msgStr = "message";

        send(ignite1.message().withAsync(), msgStr, new ProcedureApply() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * Test for check, that if use default mode, local listeners execute
     * in the same thread. 2 node in topology.
     */
    public void testSendDefaultMode2Node() throws Exception {
        final String msgStr = "message";

        sendWith2Node(ignite1.message(), msgStr, new ProcedureApply() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertEquals(Thread.currentThread(), thread);
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * Test for check, that if use async mode, local listeners execute
     * in another thread(through pool). 2 node in topology.
     */
    public void testSendAsyncMode2Node() throws Exception {
        final String msgStr = "message";

        sendWith2Node(ignite1.message().withAsync(), msgStr, new ProcedureApply() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * @param igniteMsg Ignite message.
     * @param msgStr    Message string.
     * @param cls       Callback for compare result.
     */
    private void sendWith2Node(
        final IgniteMessaging igniteMsg,
        final String msgStr,
        final ProcedureApply cls
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        ignite2.message().localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msg) {
                Assert.assertEquals(msgStr, msg);
                latch.countDown();
                return false;
            }
        });

        send(igniteMsg, msgStr, cls);

        latch.await();
    }

    /**
     * @param igniteMsg Ignite message.
     * @param msgStr    Message string.
     * @param cls       Callback for compare result.
     */
    private void send(
        IgniteMessaging igniteMsg,
        String msgStr,
        ProcedureApply cls
    ) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<Thread> thread = new AtomicReference<>();
        final AtomicReference<String> val = new AtomicReference<>();

        igniteMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msgStr) {
                thread.set(Thread.currentThread());
                val.set(msgStr);
                latch.countDown();
                return false;
            }
        });

        igniteMsg.send(TOPIC, msgStr);

        latch.await();

        cls.apply(val.get(), thread.get());
    }

    /**
     * Only for this test procedure.
     */
    private interface ProcedureApply {
        /**
         * @param val    Value.
         * @param thread Thread.
         */
        void apply(String val, Thread thread);
    }
}
