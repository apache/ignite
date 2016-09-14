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

import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class IgniteMessagingSendAsyncTest extends GridCommonAbstractTest implements Serializable {
    /**
     * Topic name.
     */
    private final String TOPIC = "topic";

    /**
     * Message string.
     */
    private final String msgStr = "message";

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
    public void testSendDefaultMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        send(ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread> () {
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
    public void testSendAsyncMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        send(ignite1.message().withAsync(), msgStr,  new IgniteBiInClosure<String, Thread> () {
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
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        sendWith2Node(ignite2, ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread> () {
            @Override public  void apply(String msg, Thread thread) {
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
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        sendWith2Node(ignite2, ignite1.message().withAsync(), msgStr,  new IgniteBiInClosure<String, Thread> () {
            @Override public  void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        });
    }

    /**
     * Test for check, that SendOrdered work in our thread pool. 1 node in topology.
     */
    public void testSendOrderedDefaultMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        final List<String> msgs = orderedMsg();

        sendOrdered(ignite1.message(), msgs, new IgniteBiInClosure< List<String>,  List<Thread>> () {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * Test for check, that SendOrdered work in our thread pool. 1 node in topology.
     */
    public void testSendOrderedAsyncMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        final List<String> msgs = orderedMsg();

        sendOrdered(ignite1.message().withAsync(), msgs, new IgniteBiInClosure< List<String>,  List<Thread>> () {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * Test for check, that SendOrdered work in our thread pool. 2 node in topology.
     */
    public void testSendOrderedDefaultMode2Node() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        final List<String> msgs = orderedMsg();

        sendOrderedWith2Node(ignite2, ignite1.message(), msgs, new IgniteBiInClosure<List<String>, List<Thread>>() {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * Test for check, that SendOrdered work in our thread pool. 2 node in topology.
     */
    public void testSendOrderedAsyncMode2Node() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        final List<String> msgs = orderedMsg();

        sendOrderedWith2Node(ignite2, ignite1.message().withAsync(), msgs, new IgniteBiInClosure<List<String>, List<Thread>>() {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * @param igniteMsg Ignite message.
     * @param msgStr    Message string.
     * @param cls       Callback for compare result.
     */
    private void sendWith2Node(
            final Ignite ignite2,
            final IgniteMessaging igniteMsg,
            final String msgStr,
            final IgniteBiInClosure<String,Thread>  cls
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        ignite2.message().localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msg) {
                Assert.assertEquals(msgStr, msg);
                latch.countDown();
                return true;
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
           final IgniteMessaging igniteMsg,
           final String msgStr,
           final IgniteBiInClosure<String,Thread> cls
    ) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final AtomicReference<Thread> thread = new AtomicReference<>();
        final AtomicReference<String> val = new AtomicReference<>();

        igniteMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msgStr) {
                thread.set(Thread.currentThread());
                val.set(msgStr);
                latch.countDown();
                return true;
            }
        });

        igniteMsg.send(TOPIC, msgStr);

        latch.await();

        cls.apply(val.get(), thread.get());
    }

    /**
     * @param ignite2 Ignite 2.
     * @param igniteMsg Ignite message.
     * @param msgs messages for send.
     * @param cls  Callback for compare result.
     */
    private void sendOrderedWith2Node(
            final Ignite ignite2,
            final IgniteMessaging igniteMsg,
            final List<String> msgs,
            final IgniteBiInClosure<List<String>,List<Thread>> cls
    ) throws Exception {

        final CountDownLatch latch = new CountDownLatch(msgs.size());

        final List<String> received = Lists.newArrayList();

        ignite2.message().localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msg) {
                received.add(msg);
                latch.countDown();
                return true;
            }
        });

        sendOrdered(igniteMsg, msgs, cls);

        latch.await();

        assertTrue(msgs.equals(received));

    }

    /**
     * @param igniteMsg Ignite message.
     * @param msgs  messages for send.
     * @param cls Callback for compare result.
     */
    private void sendOrdered(
            final IgniteMessaging igniteMsg,
            final List<String> msgs,
            final IgniteBiInClosure<List<String>,List<Thread>> cls
    ) throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(msgs.size());

        final List<String> received = Lists.newArrayList();
        final List<Thread> threads = Lists.newArrayList();

        for (String msg : msgs)
            igniteMsg.sendOrdered(TOPIC, msg, 1000);

        igniteMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String s) {
                received.add(s);
                threads.add(Thread.currentThread());
                latch.countDown();
                return true;
            }
        });

        latch.await();

        cls.apply(received, threads);
    }

    /**
     * @return List ordered messages
     */
    private List<String> orderedMsg() {
        final List<String> msgs = Lists.newArrayList();

        for (int i = 0; i < 1000; i++)
            msgs.add("" + i);

        return msgs;
    }
}
