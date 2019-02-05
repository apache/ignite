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

package org.apache.ignite.stream.jms11;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.security.SimpleAuthenticationPlugin;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link JmsStreamer}. Tests both queues and topics.
 *
 * @author Raul Kripalani
 */
public class IgniteJmsStreamerTest extends GridCommonAbstractTest {
    /** */
    private static final int CACHE_ENTRY_COUNT = 100;

    /** */
    private static final String QUEUE_NAME = "ignite.test.queue";

    /** */
    private static final String TOPIC_NAME = "ignite.test.topic";

    /** */
    private static final Map<String, String> TEST_DATA = new HashMap<>();

    static {
        for (int i = 1; i <= CACHE_ENTRY_COUNT; i++)
            TEST_DATA.put(Integer.toString(i), "v" + i);
    }

    /** */
    private BrokerService broker;

    /** */
    private ConnectionFactory connFactory;

    /** Constructor. */
    public IgniteJmsStreamerTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Override public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setPersistenceAdapter(null);
        broker.setPersistenceFactory(null);

        PolicyMap plcMap = new PolicyMap();
        PolicyEntry plc = new PolicyEntry();

        plc.setQueuePrefetch(1);

        broker.setDestinationPolicy(plcMap);
        broker.getDestinationPolicy().setDefaultEntry(plc);
        broker.setSchedulerSupport(false);

        broker.start(true);

        connFactory = new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());
    }

    /**
     * @throws Exception Iff ailed.
     */
    @Override public void afterTest() throws Exception {
        grid().cache(DEFAULT_CACHE_NAME).clear();

        broker.stop();
        broker.deleteAllMessages();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueFromName() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestinationType(Queue.class);
            jmsStreamer.setDestinationName(QUEUE_NAME);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopicFromName() throws JMSException, InterruptedException {
        Destination dest = new ActiveMQTopic(TOPIC_NAME);

        // should not produced messages until subscribed to the topic; otherwise they will be missed because this is not
        // a durable subscriber (for which a dedicated test exists)

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestinationType(Topic.class);
            jmsStreamer.setDestinationName(TOPIC_NAME);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // produce messages
            produceObjectMessages(dest, false);

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueFromExplicitDestination() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            // start the streamer
            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopicFromExplicitDestination() throws JMSException, InterruptedException {
        Destination dest = new ActiveMQTopic(TOPIC_NAME);

        // should not produced messages until subscribed to the topic; otherwise they will be missed because this is not
        // a durable subscriber (for which a dedicated test exists)

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // produce messages
            produceObjectMessages(dest, false);

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInsertMultipleCacheEntriesFromOneMessage() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce A SINGLE MESSAGE, containing all data, into the queue
        produceStringMessages(dest, true);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDurableSubscriberStartStopStart() throws Exception {
        Destination dest = new ActiveMQTopic(TOPIC_NAME);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);
            jmsStreamer.setDurableSubscription(true);
            jmsStreamer.setClientId(Long.toString(System.currentTimeMillis()));
            jmsStreamer.setDurableSubscriptionName("ignite-test-durable");

            // we start the streamer so that the durable subscriber registers itself
            jmsStreamer.start();

            // we stop it immediately
            jmsStreamer.stop();

            // we assert that there are no clients of the broker (to make sure we disconnected properly)
            assertEquals(0, broker.getCurrentConnections());

            // we send messages while we're still away
            produceStringMessages(dest, false);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueMessagesConsumedInBatchesCompletionSizeBased() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);
            jmsStreamer.setBatched(true);
            jmsStreamer.setBatchClosureSize(99);

            // disable time-based session commits
            jmsStreamer.setBatchClosureMillis(0);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            // we expect all entries to be loaded, but still one (uncommitted) message should remain in the queue
            // as observed by the broker
            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(dest).getDestinationStatistics();
            assertEquals(1, qStats.getMessages().getCount());
            assertEquals(1, qStats.getInflight().getCount());

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueMessagesConsumedInBatchesCompletionTimeBased() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);
            jmsStreamer.setBatched(true);
            jmsStreamer.setBatchClosureMillis(2000);
            // disable size-based session commits
            jmsStreamer.setBatchClosureSize(0);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);
            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(dest).getDestinationStatistics();

            jmsStreamer.start();

            // all messages are still inflight
            assertEquals(CACHE_ENTRY_COUNT, qStats.getMessages().getCount());
            assertEquals(0, qStats.getDequeues().getCount());

            // wait a little bit
            Thread.sleep(100);

            // all messages are still inflight
            assertEquals(CACHE_ENTRY_COUNT, qStats.getMessages().getCount());
            assertEquals(0, qStats.getDequeues().getCount());

            // now let the scheduler execute
            Thread.sleep(2100);

            // all messages are committed
            assertEquals(0, qStats.getMessages().getCount());
            assertEquals(CACHE_ENTRY_COUNT, qStats.getDequeues().getCount());

            latch.await(5, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGenerateNoEntries() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            // override the transformer with one that generates no cache entries
            jmsStreamer.setTransformer(TestTransformers.generateNoEntries());
            jmsStreamer.setDestination(dest);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(1);

            jmsStreamer.start();

            // no cache PUT events were received in 3 seconds, i.e. CountDownLatch does not fire
            assertFalse(latch.await(3, TimeUnit.SECONDS));

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTransactedSessionNoBatching() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setTransacted(true);
            jmsStreamer.setDestination(dest);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testQueueMultipleThreads() throws Exception {
        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(dest, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(dest);
            jmsStreamer.setThreads(5);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            // start the streamer
            jmsStreamer.start();

            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(dest).getDestinationStatistics();
            assertEquals(5, qStats.getConsumers().getCount());

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            // assert that all consumers received messages - given that the prefetch is 1
            for (Subscription subscription : broker.getBroker().getDestinationMap().get(dest).getConsumers())
                assertTrue(subscription.getDequeueCounter() > 0);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    /**
     * Test for ExceptionListener functionality.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testExceptionListener() throws Exception {
        // restart broker with auth plugin
        if (broker.isStarted())
            broker.stop();

        broker.waitUntilStopped();

        broker.setPlugins(new BrokerPlugin[]{new SimpleAuthenticationPlugin(new ArrayList())});

        broker.start(true);

        connFactory = new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());

        final List<Throwable> lsnrExceptions = new LinkedList<>();

        final CountDownLatch latch = new CountDownLatch(1);

        Destination dest = new ActiveMQQueue(QUEUE_NAME);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);

            jmsStreamer.setExceptionListener(new ExceptionListener() {
                @Override public void onException(JMSException e) {
                    System.out.println("ERROR");

                    lsnrExceptions.add(e);

                    latch.countDown();
                }
            });

            jmsStreamer.setDestination(dest);

            GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    jmsStreamer.start();

                    return null;
                }
            }, SecurityException.class);

            assertTrue(latch.await(10, TimeUnit.SECONDS));

            assertTrue(lsnrExceptions.size() > 0);

            GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    jmsStreamer.stop();

                    return null;
                }
            }, IgniteException.class);
        }
    }

    /**
     *
     */
    private void assertAllCacheEntriesLoaded() {
        // Get the cache and check that the entries are present
        IgniteCache<String, String> cache = grid().cache(DEFAULT_CACHE_NAME);
        for (Map.Entry<String, String> entry : TEST_DATA.entrySet())
            assertEquals(entry.getValue(), cache.get(entry.getKey()));
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> JmsStreamer<T, String, String> newJmsStreamer(Class<T> type,
        IgniteDataStreamer<String, String> dataStreamer) {

        JmsStreamer<T, String, String> jmsStreamer = new JmsStreamer<>();
        jmsStreamer.setIgnite(grid());
        jmsStreamer.setStreamer(dataStreamer);
        jmsStreamer.setConnectionFactory(connFactory);

        if (type == ObjectMessage.class)
            jmsStreamer.setTransformer((MessageTransformer<T, String, String>) TestTransformers.forObjectMessage());
        else
            jmsStreamer.setTransformer((MessageTransformer<T, String, String>) TestTransformers.forTextMessage());

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(10);
        return jmsStreamer;
    }

    /**
     * @param expect Expected events number.
     * @return Event receive latch.
     */
    private CountDownLatch subscribeToPutEvents(int expect) {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items
        final CountDownLatch latch = new CountDownLatch(expect);

        @SuppressWarnings("serial") IgniteBiPredicate<UUID, CacheEvent> cb = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                latch.countDown();
                return true;
            }
        };

        ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).remoteListen(cb, null, EVT_CACHE_OBJECT_PUT);
        return latch;
    }

    private void produceObjectMessages(Destination dest, boolean singleMsg) throws JMSException {
        Session ses = connFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer mp = ses.createProducer(dest);

        HashSet<TestTransformers.TestObject> set = new HashSet<>();

        for (String key : TEST_DATA.keySet()) {
            TestTransformers.TestObject to = new TestTransformers.TestObject(key, TEST_DATA.get(key));
            set.add(to);
        }

        int messagesSent;

        if (singleMsg) {
            mp.send(ses.createObjectMessage(set));
            messagesSent = 1;
        }
        else {
            for (TestTransformers.TestObject to : set)
                mp.send(ses.createObjectMessage(to));

            messagesSent = set.size();
        }

        if (dest instanceof Queue) {
            try {
                assertEquals(messagesSent, broker.getBroker().getDestinationMap().get(dest)
                    .getDestinationStatistics().getMessages().getCount());
            }
            catch (Exception e) {
                fail(e.toString());
            }
        }

    }

    private void produceStringMessages(Destination dest, boolean singleMsg) throws JMSException {
        Session ses = connFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer mp = ses.createProducer(dest);

        HashSet<String> set = new HashSet<>();

        for (String key : TEST_DATA.keySet())
            set.add(key + "," + TEST_DATA.get(key));

        int messagesSent;

        if (singleMsg) {
            StringBuilder sb = new StringBuilder();

            for (String s : set)
                sb.append(s).append("|");

            sb.deleteCharAt(sb.length() - 1);
            mp.send(ses.createTextMessage(sb.toString()));
            messagesSent = 1;

        }
        else {
            for (String s : set)
                mp.send(ses.createTextMessage(s));

            messagesSent = set.size();
        }

        if (dest instanceof Queue) {
            try {
                assertEquals(messagesSent, broker.getBroker().getDestinationMap().get(dest)
                    .getDestinationStatistics().getMessages().getCount());
            }
            catch (Exception e) {
                fail(e.toString());
            }
        }

    }
}
