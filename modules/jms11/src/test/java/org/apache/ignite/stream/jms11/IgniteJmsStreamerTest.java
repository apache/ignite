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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link JmsStreamer}. Tests both queues and topics.
 *
 * @author Raul Kripalani
 */
public class IgniteJmsStreamerTest extends GridCommonAbstractTest {

    private static final int CACHE_ENTRY_COUNT = 100;
    private static final String QUEUE_NAME = "ignite.test.queue";
    private static final String TOPIC_NAME = "ignite.test.topic";
    private static final Map<String, String> TEST_DATA = new HashMap<>();

    static {
        for (int i = 1; i <= CACHE_ENTRY_COUNT; i++)
            TEST_DATA.put(Integer.toString(i), "v" + i);
    }

    private BrokerService broker;
    private ConnectionFactory connectionFactory;

    /** Constructor. */
    public IgniteJmsStreamerTest() {
        super(true);
    }

    @Before @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        broker = new BrokerService();
        broker.deleteAllMessages();
        broker.setPersistent(false);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setQueuePrefetch(1);
        broker.setDestinationPolicy(policyMap);
        broker.getDestinationPolicy().setDefaultEntry(policy);

        broker.start(true);

        connectionFactory = new ActiveMQConnectionFactory(BrokerRegistry.getInstance().findFirst().getVmConnectorURI());

    }

    @After
    public void afterTest() throws Exception {
        grid().cache(null).clear();

        broker.deleteAllMessages();
        broker.stop();
    }

    public void testQueueFromName() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
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

    public void testTopicFromName() throws JMSException, InterruptedException {
        Destination destination = new ActiveMQTopic(TOPIC_NAME);

        // should not produced messages until subscribed to the topic; otherwise they will be missed because this is not
        // a durable subscriber (for which a dedicated test exists)

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestinationType(Topic.class);
            jmsStreamer.setDestinationName(TOPIC_NAME);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // produce messages
            produceObjectMessages(destination, false);

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    public void testQueueFromExplicitDestination() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);

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

    public void testTopicFromExplicitDestination() throws JMSException, InterruptedException {
        Destination destination = new ActiveMQTopic(TOPIC_NAME);

        // should not produced messages until subscribed to the topic; otherwise they will be missed because this is not
        // a durable subscriber (for which a dedicated test exists)

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // produce messages
            produceObjectMessages(destination, false);

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    public void testInsertMultipleCacheEntriesFromOneMessage() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce A SINGLE MESSAGE, containing all data, into the queue
        produceStringMessages(destination, true);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    public void testDurableSubscriberStartStopStart() throws Exception {
        Destination destination = new ActiveMQTopic(TOPIC_NAME);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);
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
            produceStringMessages(destination, false);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    public void testQueueMessagesConsumedInBatchesCompletionSizeBased() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);
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
            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(destination).getDestinationStatistics();
            assertEquals(1, qStats.getMessages().getCount());
            assertEquals(1, qStats.getInflight().getCount());

            jmsStreamer.stop();
        }

    }

    public void testQueueMessagesConsumedInBatchesCompletionTimeBased() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);
            jmsStreamer.setBatched(true);
            jmsStreamer.setBatchClosureMillis(2000);
            // disable size-based session commits
            jmsStreamer.setBatchClosureSize(0);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);
            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(destination).getDestinationStatistics();

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

    public void testGenerateNoEntries() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            // override the transformer with one that generates no cache entries
            jmsStreamer.setTransformer(TestTransformers.generateNoEntries());
            jmsStreamer.setDestination(destination);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(1);

            jmsStreamer.start();

            // no cache PUT events were received in 3 seconds, i.e. CountDownLatch does not fire
            assertFalse(latch.await(3, TimeUnit.SECONDS));

            jmsStreamer.stop();
        }

    }

    public void testTransactedSessionNoBatching() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce multiple messages into the queue
        produceStringMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<TextMessage, String, String> jmsStreamer = newJmsStreamer(TextMessage.class, dataStreamer);
            jmsStreamer.setTransacted(true);
            jmsStreamer.setDestination(destination);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            jmsStreamer.start();

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    public void testQueueMultipleThreads() throws Exception {
        Destination destination = new ActiveMQQueue(QUEUE_NAME);

        // produce messages into the queue
        produceObjectMessages(destination, false);

        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            JmsStreamer<ObjectMessage, String, String> jmsStreamer = newJmsStreamer(ObjectMessage.class, dataStreamer);
            jmsStreamer.setDestination(destination);
            jmsStreamer.setThreads(5);

            // subscribe to cache PUT events and return a countdown latch starting at CACHE_ENTRY_COUNT
            CountDownLatch latch = subscribeToPutEvents(CACHE_ENTRY_COUNT);

            // start the streamer
            jmsStreamer.start();

            DestinationStatistics qStats = broker.getBroker().getDestinationMap().get(destination).getDestinationStatistics();
            assertEquals(5, qStats.getConsumers().getCount());

            // all cache PUT events received in 10 seconds
            latch.await(10, TimeUnit.SECONDS);

            // assert that all consumers received messages - given that the prefetch is 1
            for (Subscription subscription : broker.getBroker().getDestinationMap().get(destination).getConsumers())
                assertTrue(subscription.getDequeueCounter() > 0);

            assertAllCacheEntriesLoaded();

            jmsStreamer.stop();
        }

    }

    private void assertAllCacheEntriesLoaded() {
        // Get the cache and check that the entries are present
        IgniteCache<String, String> cache = grid().cache(null);
        for (Map.Entry<String, String> entry : TEST_DATA.entrySet())
            assertEquals(entry.getValue(), cache.get(entry.getKey()));
    }

    @SuppressWarnings("unchecked")
    private <T extends Message> JmsStreamer<T, String, String> newJmsStreamer(Class<T> type,
        IgniteDataStreamer<String, String> dataStreamer) {

        JmsStreamer<T, String, String> jmsStreamer = new JmsStreamer<>();
        jmsStreamer.setIgnite(grid());
        jmsStreamer.setStreamer(dataStreamer);
        jmsStreamer.setConnectionFactory(connectionFactory);

        if (type == ObjectMessage.class) {
            jmsStreamer.setTransformer((MessageTransformer<T, String, String>) TestTransformers.forObjectMessage());
        }
        else {
            jmsStreamer.setTransformer((MessageTransformer<T, String, String>) TestTransformers.forTextMessage());
        }

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(10);
        return jmsStreamer;
    }

    private CountDownLatch subscribeToPutEvents(int expect) {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items
        final CountDownLatch latch = new CountDownLatch(expect);
        @SuppressWarnings("serial") IgniteBiPredicate<UUID, CacheEvent> callback = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                latch.countDown();
                return true;
            }
        };

        ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(callback, null, EVT_CACHE_OBJECT_PUT);
        return latch;
    }

    private void produceObjectMessages(Destination destination, boolean singleMessage) throws JMSException {
        Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer mp = session.createProducer(destination);
        HashSet<TestTransformers.TestObject> set = new HashSet<>();

        for (String key : TEST_DATA.keySet()) {
            TestTransformers.TestObject to = new TestTransformers.TestObject(key, TEST_DATA.get(key));
            set.add(to);
        }

        int messagesSent;
        if (singleMessage) {
            mp.send(session.createObjectMessage(set));
            messagesSent = 1;
        }
        else {
            for (TestTransformers.TestObject to : set)
                mp.send(session.createObjectMessage(to));

            messagesSent = set.size();
        }

        if (destination instanceof Queue) {
            try {
                assertEquals(messagesSent, broker.getBroker().getDestinationMap().get(destination)
                    .getDestinationStatistics().getMessages().getCount());
            }
            catch (Exception e) {
                fail(e.toString());
            }
        }

    }

    private void produceStringMessages(Destination destination, boolean singleMessage) throws JMSException {
        Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer mp = session.createProducer(destination);
        HashSet<String> set = new HashSet<>();

        for (String key : TEST_DATA.keySet())
            set.add(key + "," + TEST_DATA.get(key));

        int messagesSent;
        if (singleMessage) {
            StringBuilder sb = new StringBuilder();

            for (String s : set)
                sb.append(s).append("|");

            sb.deleteCharAt(sb.length() - 1);
            mp.send(session.createTextMessage(sb.toString()));
            messagesSent = 1;

        }
        else {
            for (String s : set) {
                mp.send(session.createTextMessage(s));
            }
            messagesSent = set.size();
        }

        if (destination instanceof Queue) {
            try {
                assertEquals(messagesSent, broker.getBroker().getDestinationMap().get(destination)
                    .getDestinationStatistics().getMessages().getCount());
            }
            catch (Exception e) {
                fail(e.toString());
            }
        }

    }

}