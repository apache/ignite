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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class IgniteMessagingSendAsyncTest extends GridCommonAbstractTest implements Serializable {
    /** Threads number for multi-thread tests. */
    private static final int THREADS = 10;

    /** */
    private final String TOPIC = "topic";

    /** */
    private final String msgStr = "message";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Checks if use default mode, local listeners execute in the same thread, 1 node in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendDefaultMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        send(ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread>() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertEquals(Thread.currentThread(), thread);
                Assert.assertEquals(msgStr, msg);
            }
        }, false);
    }

    /**
     * Checks if use async mode, local listeners execute in another thread, 1 node in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendAsyncMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        send(ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread>() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        }, true);
    }

    /**
     * Checks if use default mode, local listeners execute in the same thread, 2 nodes in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendDefaultMode2Nodes() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        sendWith2Nodes(ignite2, ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread>() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertEquals(Thread.currentThread(), thread);
                Assert.assertEquals(msgStr, msg);
            }
        }, false);
    }

    /**
     * Checks if use async mode, local listeners execute in another thread, 2 nodes in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendAsyncMode2Node() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        sendWith2Nodes(ignite2, ignite1.message(), msgStr, new IgniteBiInClosure<String, Thread>() {
            @Override public void apply(String msg, Thread thread) {
                Assert.assertTrue(!Thread.currentThread().equals(thread));
                Assert.assertEquals(msgStr, msg);
            }
        }, true);
    }

    /**
     * Checks that sendOrdered works in thread pool, 1 node in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendOrderedDefaultMode() throws Exception {
        Ignite ignite1 = startGrid(1);

        final List<String> msgs = orderedMessages();

        sendOrdered(ignite1.message(), msgs, new IgniteBiInClosure<List<String>, List<Thread>>() {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * Checks that sendOrdered work in thread pool, 2 nodes in topology.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSendOrderedDefaultMode2Node() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        final List<String> msgs = orderedMessages();

        sendOrderedWith2Node(ignite2, ignite1.message(), msgs, new IgniteBiInClosure<List<String>, List<Thread>>() {
            @Override public void apply(List<String> received, List<Thread> threads) {
                assertFalse(threads.contains(Thread.currentThread()));
                assertTrue(msgs.equals(received));
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendOrderedDefaultModeMultiThreads() throws Exception {
        Ignite ignite = startGrid(1);

        sendOrderedMultiThreads(ignite.message());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSendOrderedDefaultModeMultiThreadsWith2Node() throws Exception {
        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        sendOrderedMultiThreadsWith2Node(ignite2, ignite1.message());
    }

    /**
     * @param ignite2 Second node.
     * @param ignMsg IgniteMessage.
     * @throws Exception If failed.
     */
    private void sendOrderedMultiThreadsWith2Node(
            final Ignite ignite2,
            final IgniteMessaging ignMsg
    ) throws Exception {
        final ConcurrentMap<String, List<String>> expMsg = Maps.newConcurrentMap();
        final ConcurrentMap<String, List<String>> actlMsg = Maps.newConcurrentMap();

        final List<String> msgs = orderedMessages();

        sendOrderedMultiThreadsWith2Node(ignite2, ignMsg, expMsg, actlMsg, msgs);

    }

    /**
     * @param ignMsg IgniteMessaging.
     * @throws Exception If failed.
     */
    private void sendOrderedMultiThreads(
            final IgniteMessaging ignMsg
    ) throws Exception {
        final ConcurrentMap<String, List<String>> expMsg = Maps.newConcurrentMap();
        final ConcurrentMap<String, List<String>> actlMsg = Maps.newConcurrentMap();

        final List<String> msgs = orderedMessages();

        sendOrderedMultiThreads(ignMsg, expMsg, actlMsg, msgs);
    }

    /**
     * @param ignite2 Second node.
     * @param ignMsg Ignite for send message.
     * @param expMsg Expected messages map.
     * @param actlMsg Actual message map.
     * @param msgs List of messages.
     * @throws Exception If failed.
     */
    private void sendOrderedMultiThreadsWith2Node(
            final Ignite ignite2,
            final IgniteMessaging ignMsg,
            final ConcurrentMap<String, List<String>> expMsg,
            final ConcurrentMap<String, List<String>> actlMsg,
            final List<String> msgs
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(THREADS * msgs.size());

        final ConcurrentMap<String, List<String>> actlMsgNode2 = Maps.newConcurrentMap();

        ignite2.message().localListen(TOPIC, new IgniteBiPredicate<UUID, Message>() {
            @Override public boolean apply(UUID uuid, Message msg) {
                actlMsgNode2.putIfAbsent(msg.threadName, Lists.<String>newArrayList());

                actlMsgNode2.get(msg.threadName).add(msg.msg);

                latch.countDown();

                return true;
            }
        });

        sendOrderedMultiThreads(ignMsg, expMsg, actlMsg, msgs);

        latch.await();

        assertEquals(expMsg.size(), actlMsgNode2.size());

        for (Map.Entry<String, List<String>> entry : expMsg.entrySet())
            assertTrue(actlMsgNode2.get(entry.getKey()).equals(entry.getValue()));
    }

    /**
     * @param ignMsg Ignite for send message.
     * @param expMsg Expected messages map.
     * @param actlMsg Actual message map.
     * @param msgs List of messages.
     * @throws Exception If failed.
     */
    private void sendOrderedMultiThreads(
            final IgniteMessaging ignMsg,
            final ConcurrentMap<String, List<String>> expMsg,
            final ConcurrentMap<String, List<String>> actlMsg,
            final List<String> msgs
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(THREADS * msgs.size());

        ignMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, Message>() {
            @Override public boolean apply(UUID uuid, Message msg) {
                actlMsg.putIfAbsent(msg.threadName, Lists.<String>newArrayList());

                actlMsg.get(msg.threadName).add(msg.msg);

                latch.countDown();

                return true;
            }
        });

        for (int i = 0; i < THREADS; i++)
            new Thread(new Runnable() {
                @Override public void run() {
                    String thdName = Thread.currentThread().getName();

                    List<String> exp = Lists.newArrayList();

                    expMsg.put(thdName, exp);

                    for (String msg : msgs) {
                        exp.add(msg);

                        ignMsg.sendOrdered(TOPIC, new Message(thdName, msg), 1000);
                    }

                }
            }).start();

        latch.await();

        assertEquals(expMsg.size(), actlMsg.size());

        for (Map.Entry<String, List<String>> entry : expMsg.entrySet())
            assertTrue(actlMsg.get(entry.getKey()).equals(entry.getValue()));
    }

    /**
     * @param ignite2 Second node.
     * @param igniteMsg Ignite message.
     * @param msgStr    Message string.
     * @param cls       Callback for compare result.
     * @param async     Use sendAsync flag.
     * @throws Exception If failed.
     */
    private void sendWith2Nodes(
            final Ignite ignite2,
            final IgniteMessaging igniteMsg,
            final String msgStr,
            final IgniteBiInClosure<String, Thread> cls,
            final boolean async
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        ignite2.message().localListen(TOPIC, new IgniteBiPredicate<UUID, String>() {
            @Override public boolean apply(UUID uuid, String msg) {
                Assert.assertEquals(msgStr, msg);

                latch.countDown();

                return true;
            }
        });

        send(igniteMsg, msgStr, cls, async);

        latch.await();
    }

    /**
     * @param igniteMsg Ignite messaging.
     * @param msgStr    Message string.
     * @param cls       Callback for compare result.
     * @param async     Use sendAsync flag.
     * @throws Exception If failed.
     */
    private void send(
           final IgniteMessaging igniteMsg,
           final String msgStr,
           final IgniteBiInClosure<String, Thread> cls,
           final boolean async
    ) throws Exception {
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

        if (async)
            igniteMsg.withAsync().send(TOPIC, msgStr);
        else
            igniteMsg.send(TOPIC, msgStr);

        latch.await();

        cls.apply(val.get(), thread.get());
    }

    /**
     * @param ignite2 Second node.
     * @param igniteMsg Ignite message.
     * @param msgs messages for send.
     * @param cls  Callback for compare result.
     * @throws Exception If failed.
     */
    private void sendOrderedWith2Node(
            final Ignite ignite2,
            final IgniteMessaging igniteMsg,
            final List<String> msgs,
            final IgniteBiInClosure<List<String>, List<Thread>> cls
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
     * @throws Exception If failed.
     */
    private <T> void sendOrdered(
            final IgniteMessaging igniteMsg,
            final List<T> msgs,
            final IgniteBiInClosure<List<T>,List<Thread>> cls
    ) throws Exception {
        final CountDownLatch latch = new CountDownLatch(msgs.size());

        final List<T> received = Lists.newArrayList();
        final List<Thread> threads = Lists.newArrayList();

        for (T msg : msgs)
            igniteMsg.sendOrdered(TOPIC, msg, 1000);

        igniteMsg.localListen(TOPIC, new IgniteBiPredicate<UUID, T>() {
            @Override public boolean apply(UUID uuid, T s) {
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
     * @return List of ordered messages
     */
    private List<String> orderedMessages() {
        final List<String> msgs = Lists.newArrayList();

        for (int i = 0; i < 1000; i++)
            msgs.add(String.valueOf(ThreadLocalRandom.current().nextInt()));

        return msgs;
    }

    /**
     *
     */
    private static class Message implements Serializable {
        /** Thread name. */
        private final String threadName;

        /** Message. */
        private final String msg;

        /**
         * @param threadName Thread name.
         * @param msg Message.
         */
        private Message(String threadName, String msg) {
            this.threadName = threadName;
            this.msg = msg;
        }
    }
}
