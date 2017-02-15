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

package org.apache.ignite.stream.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import kafka.consumer.ConsumerConfig;
import kafka.message.MessageAndMetadata;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.clients.producer.ProducerRecord;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link KafkaStreamer}.
 */
public class KafkaIgniteStreamerSelfTest extends GridCommonAbstractTest {
    /** Embedded Kafka. */
    private TestKafkaBroker embeddedBroker;

    /** Count. */
    private static final int CNT = 100;

    /** Test topic. */
    private static final String TOPIC_NAME = "page_visits";

    /** Kafka partition. */
    private static final int PARTITIONS = 4;

    /** Kafka replication factor. */
    private static final int REPLICATION_FACTOR = 1;

    /** Topic message key prefix. */
    private static final String KEY_PREFIX = "192.168.2.";

    /** Topic message value URL. */
    private static final String VALUE_URL = ",www.example.com,";

    /** Constructor. */
    public KafkaIgniteStreamerSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        embeddedBroker = new TestKafkaBroker();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(null).clear();

        embeddedBroker.shutdown();
    }

    /**
     * Tests Kafka streamer.
     *
     * @throws TimeoutException If timed out.
     * @throws InterruptedException If interrupted.
     */
    public void testKafkaStreamer() throws TimeoutException, InterruptedException {
        embeddedBroker.createTopic(TOPIC_NAME, PARTITIONS, REPLICATION_FACTOR);

        Map<String, String> keyValMap = produceStream(TOPIC_NAME);

        consumerStream(TOPIC_NAME, keyValMap);
    }

    /**
     * Sends messages to Kafka.
     *
     * @param topic Topic name.
     * @return Map of key value messages.
     */
    private Map<String, String> produceStream(String topic) {
        // Generate random subnets.
        List<Integer> subnet = new ArrayList<>();

        for (int i = 1; i <= CNT; i++)
            subnet.add(i);

        Collections.shuffle(subnet);

        List<ProducerRecord<String, String>> messages = new ArrayList<>(CNT);

        Map<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < CNT; evt++) {
            long runtime = System.currentTimeMillis();

            String ip = KEY_PREFIX + subnet.get(evt);

            String msg = runtime + VALUE_URL + ip;

            messages.add(new ProducerRecord<>(topic, ip, msg));

            keyValMap.put(ip, msg);
        }

        embeddedBroker.sendMessages(messages);

        return keyValMap;
    }

    /**
     * Consumes Kafka stream via Ignite.
     *
     * @param topic Topic name.
     * @param keyValMap Expected key value map.
     * @throws TimeoutException If timed out.
     * @throws InterruptedException If interrupted.
     */
    private void consumerStream(String topic, Map<String, String> keyValMap)
        throws TimeoutException, InterruptedException {
        KafkaStreamer<String, String> kafkaStmr = null;

        Ignite ignite = grid();

        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(null)) {
            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            // Configure Kafka streamer.
            kafkaStmr = new KafkaStreamer<>();

            // Get the cache.
            IgniteCache<String, String> cache = ignite.cache(null);

            // Set Ignite instance.
            kafkaStmr.setIgnite(ignite);

            // Set data streamer instance.
            kafkaStmr.setStreamer(stmr);

            // Set the topic.
            kafkaStmr.setTopic(topic);

            // Set the number of threads.
            kafkaStmr.setThreads(4);

            // Set the consumer configuration.
            kafkaStmr.setConsumerConfig(
                createDefaultConsumerConfig(embeddedBroker.getZookeeperAddress(), "groupX"));

            kafkaStmr.setMultipleTupleExtractor(
                new StreamMultipleTupleExtractor<MessageAndMetadata<byte[], byte[]>, String, String>() {
                @Override public Map<String, String> extract(MessageAndMetadata<byte[], byte[]> msg) {
                    Map<String, String> entries = new HashMap<>();

                    try {
                        String key = new String(msg.key());
                        String val = new String(msg.message());

                        // Convert the message into number of cache entries with same key or dynamic key from actual message.
                        // For now using key as cache entry key and value as cache entry value - for test purpose.
                        entries.put(key, val);
                    }
                    catch (Exception ex) {
                        fail("Unexpected error." + ex);
                    }

                    return entries;
                }
            });

            // Start kafka streamer.
            kafkaStmr.start();

            final CountDownLatch latch = new CountDownLatch(CNT);

            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);

            // Checks all events successfully processed in 10 seconds.
            assertTrue(latch.await(10, TimeUnit.SECONDS));

            for (Map.Entry<String, String> entry : keyValMap.entrySet())
                assertEquals(entry.getValue(), cache.get(entry.getKey()));
        }
        finally {
            if (kafkaStmr != null)
                kafkaStmr.stop();
        }
    }

    /**
     * Creates default consumer config.
     *
     * @param zooKeeper ZooKeeper address &lt;server:port&gt;.
     * @param grpId Group Id for kafka subscriber.
     * @return Kafka consumer configuration.
     */
    private ConsumerConfig createDefaultConsumerConfig(String zooKeeper, String grpId) {
        A.notNull(zooKeeper, "zookeeper");
        A.notNull(grpId, "groupId");

        Properties props = new Properties();

        props.put("zookeeper.connect", zooKeeper);
        props.put("group.id", grpId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }
}
