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

package org.apache.ignite.stream.pubsub;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.stream.pubsub.MockPubSubServer.PROJECT;
import static org.apache.ignite.stream.pubsub.MockPubSubServer.TOPIC_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PubSubStreamer}.
 */
public class PubSubStreamerSelfTest {
    /** Cache name. */
    private static final String DEFAULT_CACHE_NAME = "testCache";

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "config/example-ignite.xml";

    /** Subscription Name. */
    private static final String SUBSCRIPTION = "ignite_subscription";

    /** Count. */
    private static final int CNT = 100;

    /** Messages per request. */
    private static final int MESSAGES_PER_REQUEST = 10;

    /** Topic message key prefix. */
    private static final String KEY_PREFIX = "192.168.2.";

    /** Topic message value URL. */
    private static final String VALUE_URL = ",www.example.com,";

    /** */
    private static final String JSON_KEY = "key";

    /** */
    private static final String JSON_VALUE = "value";

    /** */
    private Ignite ignite;

    /** */
    private static MockPubSubServer mockPubSubServer = new MockPubSubServer();

    /** */
    @Before
    public void beforeTest() throws InterruptedException {
        this.ignite = Ignition.start(GRID_CONF_FILE);
        IgniteCache<Integer, String> igniteCache = ignite.getOrCreateCache(defaultCacheConfiguration());
    }

    /** */
    @After
    public void afterTest() {
        ignite.cache(DEFAULT_CACHE_NAME).clear();
        Ignition.stop(true);
    }

    /**
     * @return New cache configuration with modified defaults.
     */
    public static CacheConfiguration<Integer, String> defaultCacheConfiguration() {
        CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        return cfg;
    }

    /**
     * Tests Pub/Sub streamer.
     *
     * @throws TimeoutException If timed out.
     * @throws InterruptedException If interrupted.
     */
    @Test
    public void testPubSubStreamer() throws Exception {
        Map<String, String> keyValMap = produceStream();
        consumerStream(ProjectTopicName.of(MockPubSubServer.PROJECT, MockPubSubServer.TOPIC_NAME), keyValMap);
    }

    /**
     * Consumes Pub/Sub stream via Ignite.
     *
     * @param topic Topic name.
     * @param keyValMap Expected key value map.
     * @throws TimeoutException If timed out.
     * @throws InterruptedException If interrupted.
     */
    private void consumerStream(ProjectTopicName topic, Map<String, String> keyValMap)
            throws InterruptedException, IOException {
        PubSubStreamer<String, String> pubSubStmr = null;

        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(MESSAGES_PER_REQUEST);

            // Configure Pub/Sub streamer.
            pubSubStmr = new PubSubStreamer<>();

            // Get the cache.
            IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

            // Set Ignite instance.
            pubSubStmr.setIgnite(ignite);

            // Set data streamer instance.
            pubSubStmr.setStreamer(stmr);

            // Set the topic.
            pubSubStmr.setTopic(Arrays.asList(topic));

            // Set subscription name
            pubSubStmr.setSubscriptionName(ProjectSubscriptionName.format(PROJECT, SUBSCRIPTION));

            // Set the number of threads.
            pubSubStmr.setThreads(4);

            // Set the SubScriber Stub settings.
            pubSubStmr.setSubscriberStubSettings(mockPubSubServer.createSubscriberStub());

            pubSubStmr.setSingleTupleExtractor(singleTupleExtractor());

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @IgniteInstanceResource
                private Ignite ig;

                @LoggerResource
                private IgniteLogger log;

                /** {@inheritDoc} */
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    if (log.isInfoEnabled()) {
                        IgniteEx igEx = (IgniteEx)ig;

                        UUID nodeId = igEx.localNode().id();

                        log.info("Receive event=" + evt + ", nodeId=" + nodeId);
                    }

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            // Start Pub/Sub streamer.
            pubSubStmr.start();

            // Checks all events successfully processed in 10 seconds.
            assertTrue("Failed to wait latch completion, still wait " + latch.getCount() + " events",
                latch.await(10, TimeUnit.SECONDS));

            for (Map.Entry<String, String> entry : keyValMap.entrySet())
                assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }
        finally {
            if (pubSubStmr != null)
                pubSubStmr.stop();
        }
    }

    /**
     * Sends messages to Pub/Sub.
     *
     * @return Map of key value messages.
     */
    private Map<String, String> produceStream() throws Exception {
        List<Integer> subnet = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            subnet.add(i);

        Collections.shuffle(subnet);

        List<PubsubMessage> messages = new ArrayList<>();

        Map<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            long runtime = System.currentTimeMillis();

            String ip = KEY_PREFIX + subnet.get(evt);

            String msg = runtime + VALUE_URL + ip;

            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty(JSON_KEY, ip);
            jsonObject.addProperty(JSON_VALUE, msg);

            ByteString byteString = ByteString.copyFrom(jsonObject.toString(), Charset.defaultCharset());

            PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                                                       .setData(byteString)
                                                       .build();

            messages.add(pubsubMessage);
            String messageId = mockPubSubServer.getPublisher(TOPIC_NAME).publish(pubsubMessage).get();
            keyValMap.put(ip, msg);

        }

        return keyValMap;
    }

    /**
     * @return {@link StreamSingleTupleExtractor} for testing.
     */
    private static StreamSingleTupleExtractor<PubsubMessage, String, String> singleTupleExtractor() {
        return new StreamSingleTupleExtractor<PubsubMessage, String, String>() {
            @Override public Map.Entry<String, String> extract(PubsubMessage msg) {
                String dataStr = msg.getData().toStringUtf8();
                JsonElement jsonElement = new JsonParser().parse(dataStr).getAsJsonObject();
                JsonObject jsonObject = jsonElement.getAsJsonObject();
                return new GridMapEntry<>(jsonObject.get(JSON_KEY).getAsString(), jsonObject.get(JSON_VALUE).getAsString());
            }
        };
    }

    /**
     * @return {@link StreamMultipleTupleExtractor} for testing.
     */
    private static StreamMultipleTupleExtractor<PubsubMessage, String, String> multipleTupleExtractor() {
        return new StreamMultipleTupleExtractor<PubsubMessage, String, String>() {
            @Override public Map<String, String> extract(PubsubMessage msg) {
                String dataStr = msg.getData().toStringUtf8();
                JsonElement jsonElement = new JsonParser().parse(dataStr).getAsJsonObject();
                JsonArray jsonArray = jsonElement.getAsJsonArray();

                final Map<String, String> answer = new HashMap<>();

                for (int i = 0; i < jsonArray.size(); i++) {
                    JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
                    String key = jsonObject.get(JSON_KEY).getAsString();
                    String value = jsonObject.get(JSON_VALUE).getAsString();

                    answer.put(key, value);
                }

                return answer;
            }
        };
    }

}
