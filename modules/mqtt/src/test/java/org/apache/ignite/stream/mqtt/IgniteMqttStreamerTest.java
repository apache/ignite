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

package org.apache.ignite.stream.mqtt;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.base.Splitter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Before;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link MqttStreamer}.
 */
public class IgniteMqttStreamerTest extends GridCommonAbstractTest {
    /** The test data. */
    private static final Map<Integer, String> TEST_DATA = new HashMap<>();

    /** Topic name for single topic tests. */
    private static final String SINGLE_TOPIC_NAME = "abc";

    /** Topic names for multiple topic tests. */
    private static final List<String> MULTIPLE_TOPIC_NAMES = Arrays.asList("def", "ghi", "jkl", "mno");

    /** The AMQ broker with an MQTT interface. */
    private BrokerService broker;

    /** The MQTT client. */
    private MqttClient client;

    /** The broker URL. */
    private String brokerUrl;

    /** The broker port. **/
    private int port;

    /** The MQTT streamer currently under test. */
    private MqttStreamer<Integer, String> streamer;

    /** The UUID of the currently active remote listener. */
    private UUID remoteLsnr;

    /** The Ignite data streamer. */
    private IgniteDataStreamer<Integer, String> dataStreamer;

    static {
        for (int i = 0; i < 100; i++)
            TEST_DATA.put(i, "v" + i);
    }

    /** Constructor. */
    public IgniteMqttStreamerTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        // find an available local port
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }

        // create the broker
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

        // add the MQTT transport connector to the broker
        broker.addConnector("mqtt://localhost:" + port);
        broker.setStartAsync(false);
        broker.start(true);

        // create the broker URL
        brokerUrl = "tcp://localhost:" + port;

        // create the client and connect
        client = new MqttClient(brokerUrl, UUID.randomUUID().toString(), new MemoryPersistence());

        client.connect();

        // create mqtt streamer
        dataStreamer = grid().dataStreamer(null);

        streamer = createMqttStreamer(dataStreamer);
    }

    /**
     * @throws Exception If failed.
     */
    @After
    public void afterTest() throws Exception {
        try {
            streamer.stop();
        }
        catch (Exception ignored) {
            // ignore if already stopped
        }

        dataStreamer.close();

        grid().cache(null).clear();

        broker.stop();
        broker.deleteAllMessages();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectDisconnect() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopic(SINGLE_TOPIC_NAME);
        streamer.setBlockUntilConnected(true);

        // action time: repeat 10 times; make sure the connection state is kept correctly every time
        for (int i = 0; i < 10; i++) {
            streamer.start();

            assertTrue(streamer.isConnected());

            streamer.stop();

            assertFalse(streamer.isConnected());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testConnectionStatusWithBrokerDisconnection() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-2255");

        // Configure streamer.
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopic(SINGLE_TOPIC_NAME);
        streamer.setBlockUntilConnected(true);
        streamer.setRetryWaitStrategy(WaitStrategies.noWait());

        streamer.start();

        // Action time: repeat 5 times; make sure the connection state is kept correctly every time.
        for (int i = 0; i < 5; i++) {
            log.info("Iteration: " + i);

            assertTrue(streamer.isConnected());

            broker.stop();

            assertFalse(streamer.isConnected());

            broker.start(true);
            broker.waitUntilStarted();

            Thread.sleep(500);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTopic_NoQoS_OneEntryPerMessage() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopic(SINGLE_TOPIC_NAME);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleTopics_NoQoS_OneEntryPerMessage() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopics(MULTIPLE_TOPIC_NAMES);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(MULTIPLE_TOPIC_NAMES, 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        assertTrue(broker.getBroker().getDestinationMap().size() >= 4);
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("def")));
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("ghi")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTopic_NoQoS_MultipleEntriesOneMessage() throws Exception {
        // configure streamer
        streamer.setMultipleTupleExtractor(multipleTupleExtractor());
        streamer.setTopic(SINGLE_TOPIC_NAME);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 0, 50, true);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleTopics_NoQoS_MultipleEntriesOneMessage() throws Exception {
        // configure streamer
        streamer.setMultipleTupleExtractor(multipleTupleExtractor());
        streamer.setTopics(MULTIPLE_TOPIC_NAMES);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(MULTIPLE_TOPIC_NAMES, 0, 50, true);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        assertTrue(broker.getBroker().getDestinationMap().size() >= 4);
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("def")));
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("ghi")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTopic_NoQoS_ConnectOptions_Durable() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopic(SINGLE_TOPIC_NAME);

        MqttConnectOptions connOptions = new MqttConnectOptions();
        connOptions.setCleanSession(false);
        streamer.setConnectOptions(connOptions);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        // explicitly stop the streamer
        streamer.stop();

        // send messages while stopped
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 50, 50, false);

        latch = subscribeToPutEvents(50);

        // start the streamer again
        streamer.start();

        // assertions - make sure that messages sent during disconnection were also received
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(100);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTopic_NoQoS_Reconnect() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setRetryWaitStrategy(WaitStrategies.noWait());
        streamer.setRetryStopStrategy(StopStrategies.neverStop());
        streamer.setTopic(SINGLE_TOPIC_NAME);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        // now shutdown the broker, wait 2 seconds and start it again
        broker.stop();
        broker.start(true);
        broker.waitUntilStarted();

        Thread.sleep(2000);

        client.connect();

        // let's ensure we have 2 connections: Ignite and our test
        assertEquals(2, broker.getTransportConnectorByScheme("mqtt").getConnections().size());

        // subscribe to cache PUT events again
        latch = subscribeToPutEvents(50);

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 50, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(100);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTopic_NoQoS_RetryOnce() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setRetryWaitStrategy(WaitStrategies.noWait());
        streamer.setRetryStopStrategy(StopStrategies.stopAfterAttempt(1));
        streamer.setTopic(SINGLE_TOPIC_NAME);

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        // now shutdown the broker, wait 2 seconds and start it again
        broker.stop();
        broker.start(true);

        broker.waitUntilStarted();

        client.connect();

        // lets send messages and ensure they are not received, because our retrier desisted
        sendMessages(Arrays.asList(SINGLE_TOPIC_NAME), 50, 50, false);

        Thread.sleep(3000);

        assertNull(grid().cache(null).get(50));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleTopics_MultipleQoS_OneEntryPerMessage() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopics(MULTIPLE_TOPIC_NAMES);
        streamer.setQualitiesOfService(Arrays.asList(1, 1, 1, 1));

        // subscribe to cache PUT events
        CountDownLatch latch = subscribeToPutEvents(50);

        // action time
        streamer.start();

        // send messages
        sendMessages(MULTIPLE_TOPIC_NAMES, 0, 50, false);

        // assertions
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);

        assertTrue(broker.getBroker().getDestinationMap().size() >= 4);
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("def")));
        assertTrue(broker.getBroker().getDestinationMap().containsKey(new ActiveMQTopic("ghi")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleTopics_MultipleQoS_Mismatch() throws Exception {
        // configure streamer
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setTopics(MULTIPLE_TOPIC_NAMES);
        streamer.setQualitiesOfService(Arrays.asList(1, 1, 1));

        try {
            streamer.start();
        }
        catch (Exception ignored) {
            return;
        }

        fail("Expected an exception reporting invalid parameters");
    }

    /**
     * @param dataStreamer Streamer.
     * @return MQTT streamer.
     */
    private MqttStreamer<Integer, String> createMqttStreamer(IgniteDataStreamer<Integer, String> dataStreamer) {
        MqttStreamer<Integer, String> streamer = new MqttStreamer<>();
        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);
        streamer.setBrokerUrl(brokerUrl);
        streamer.setClientId(UUID.randomUUID().toString());
        streamer.setBlockUntilConnected(true);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /**
     * @param topics Topics.
     * @param fromIdx From index.
     * @param cnt Count.
     * @param singleMsg Single message flag.
     * @throws MqttException If failed.
     */
    private void sendMessages(final List<String> topics, int fromIdx, int cnt, boolean singleMsg)
        throws MqttException {
        if (singleMsg) {
            final List<StringBuilder> sbs = new ArrayList<>(topics.size());

            // initialize String Builders for each topic
            F.forEach(topics, new IgniteInClosure<String>() {
                @Override public void apply(String s) {
                    sbs.add(new StringBuilder());
                }
            });

            // fill String Builders for each topic
            F.forEach(F.range(fromIdx, fromIdx + cnt), new IgniteInClosure<Integer>() {
                @Override public void apply(Integer integer) {
                    sbs.get(integer % topics.size()).append(integer.toString() + "," + TEST_DATA.get(integer) + "\n");
                }
            });

            // send each buffer out
            for (int i = 0; i < topics.size(); i++) {
                MqttMessage msg = new MqttMessage(sbs.get(i).toString().getBytes());

                client.publish(topics.get(i % topics.size()), msg);
            }
        }
        else {
            for (int i = fromIdx; i < fromIdx + cnt; i++) {
                byte[] payload = (i + "," + TEST_DATA.get(i)).getBytes();

                MqttMessage msg = new MqttMessage(payload);

                client.publish(topics.get(i % topics.size()), msg);
            }
        }
    }

    /**
     * @param expect Expected count.
     * @return Latch to be counted down in listener.
     */
    private CountDownLatch subscribeToPutEvents(int expect) {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items
        final CountDownLatch latch = new CountDownLatch(expect);

        IgniteBiPredicate<UUID, CacheEvent> cb = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                latch.countDown();

                return true;
            }
        };

        remoteLsnr = ignite.events(ignite.cluster().forCacheNodes(null))
            .remoteListen(cb, null, EVT_CACHE_OBJECT_PUT);

        return latch;
    }

    /**
     * @param cnt Count.
     */
    private void assertCacheEntriesLoaded(int cnt) {
        // get the cache and check that the entries are present
        IgniteCache<Integer, String> cache = grid().cache(null);

        // for each key from 0 to count from the TEST_DATA (ordered by key), check that the entry is present in cache
        for (Integer key : new ArrayList<>(new TreeSet<>(TEST_DATA.keySet())).subList(0, cnt))
            assertEquals(TEST_DATA.get(key), cache.get(key));

        // assert that the cache exactly the specified amount of elements
        assertEquals(cnt, cache.size(CachePeekMode.ALL));

        // remove the event listener
        grid().events(grid().cluster().forCacheNodes(null)).stopRemoteListen(remoteLsnr);
    }

    /**
     * @return {@link StreamSingleTupleExtractor} for testing.
     */
    public static StreamSingleTupleExtractor<MqttMessage, Integer, String> singleTupleExtractor() {
        return new StreamSingleTupleExtractor<MqttMessage, Integer, String>() {
            @Override public Map.Entry<Integer, String> extract(MqttMessage msg) {
                List<String> s = Splitter.on(",").splitToList(new String(msg.getPayload()));

                return new GridMapEntry<>(Integer.parseInt(s.get(0)), s.get(1));
            }
        };
    }

    /**
     * @return {@link StreamMultipleTupleExtractor} for testing.
     */
    public static StreamMultipleTupleExtractor<MqttMessage, Integer, String> multipleTupleExtractor() {
        return new StreamMultipleTupleExtractor<MqttMessage, Integer, String>() {
            @Override public Map<Integer, String> extract(MqttMessage msg) {
                final Map<String, String> map = Splitter.on("\n")
                    .omitEmptyStrings()
                    .withKeyValueSeparator(",")
                    .split(new String(msg.getPayload()));

                final Map<Integer, String> answer = new HashMap<>();

                F.forEach(map.keySet(), new IgniteInClosure<String>() {
                    @Override public void apply(String s) {
                        answer.put(Integer.parseInt(s), map.get(s));
                    }
                });

                return answer;
            }
        };
    }

}
