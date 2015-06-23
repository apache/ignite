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

import org.apache.ignite.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import kafka.consumer.*;
import kafka.producer.*;
import kafka.serializer.*;
import kafka.utils.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Tests {@link KafkaStreamer}.
 */
public class KafkaIgniteStreamerSelfTest
    extends GridCommonAbstractTest {
    /** Embedded Kafka. */
    private KafkaEmbeddedBroker embeddedBroker;

    /** Count. */
    private static final int CNT = 100;

    /** Test Topic. */
    private static final String TOPIC_NAME = "page_visits";

    /** Kafka Partition. */
    private static final int PARTITIONS = 4;

    /** Kafka Replication Factor. */
    private static final int REPLICATION_FACTOR = 1;

    /** Topic Message Key Prefix. */
    private static final String KEY_PREFIX = "192.168.2.";

    /** Topic Message Value Url. */
    private static final String VALUE_URL = ",www.example.com,";

    /** Constructor. */
    public KafkaIgniteStreamerSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override
    protected void beforeTest()
        throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        embeddedBroker = new KafkaEmbeddedBroker();
    }

    /** {@inheritDoc} */
    @Override
    protected void afterTest()
        throws Exception {
        grid().cache(null).clear();

        embeddedBroker.shutdown();
    }

    /**
     * Tests Kafka streamer.
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testKafkaStreamer()
        throws TimeoutException, InterruptedException {
        embeddedBroker.createTopic(TOPIC_NAME, PARTITIONS, REPLICATION_FACTOR);

        Map<String, String> keyValueMap = produceStream(TOPIC_NAME);
        consumerStream(TOPIC_NAME, keyValueMap);
    }

    /**
     * Produces/Sends messages to Kafka.
     *
     * @param topic Topic name.
     * @return Map of key value messages.
     */
    private Map<String, String> produceStream(final String topic) {
        final Map<String, String> keyValueMap = new HashMap<>();

        // Generate random subnets.
        List<Integer> subnet = new ArrayList<>();

        int i = 0;
        while (i <= CNT)
            subnet.add(++i);

        Collections.shuffle(subnet);

        final List<KeyedMessage<String, String>> messages = new ArrayList<>();
        for (int event = 0; event < CNT; event++) {
            long runtime = new Date().getTime();
            String ip = KEY_PREFIX + subnet.get(event);
            String msg = runtime + VALUE_URL + ip;
            messages.add(new KeyedMessage<>(topic, ip, msg));
            keyValueMap.put(ip, msg);
        }

        final Producer<String, String> producer = embeddedBroker.sendMessages(messages);
        producer.close();

        return keyValueMap;
    }

    /**
     * Consumes Kafka Stream via ignite.
     *
     * @param topic Topic name.
     * @param keyValueMap Expected key value map.
     * @throws TimeoutException TimeoutException.
     * @throws InterruptedException InterruptedException.
     */
    private void consumerStream(final String topic, final Map<String, String> keyValueMap)
        throws TimeoutException, InterruptedException {

        KafkaStreamer<String, String, String> kafkaStmr = null;

        final Ignite ignite = grid();
        try (IgniteDataStreamer<String, String> stmr = ignite.dataStreamer(null)) {

            stmr.allowOverwrite(true);
            stmr.autoFlushFrequency(10);

            // Configure socket streamer.
            kafkaStmr = new KafkaStreamer<>();

            // Get the cache.
            IgniteCache<String, String> cache = ignite.cache(null);

            // Set ignite instance.
            kafkaStmr.setIgnite(ignite);

            // Set data streamer instance.
            kafkaStmr.setStreamer(stmr);

            // Set the topic.
            kafkaStmr.setTopic(topic);

            // Set the number of threads.
            kafkaStmr.setThreads(4);

            // Set the consumer configuration.
            kafkaStmr.setConsumerConfig(createDefaultConsumerConfig(KafkaEmbeddedBroker.getZKAddress(),
                "groupX"));

            // Set the decoders.
            StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());
            kafkaStmr.setKeyDecoder(stringDecoder);
            kafkaStmr.setValueDecoder(stringDecoder);

            // Start kafka streamer.
            kafkaStmr.start();

            final CountDownLatch latch = new CountDownLatch(CNT);
            IgniteBiPredicate<UUID, CacheEvent> locLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override
                public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();
                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(locLsnr, null, EVT_CACHE_OBJECT_PUT);
            latch.await();

            for (Map.Entry<String, String> entry : keyValueMap.entrySet()) {
                final String key = entry.getKey();
                final String value = entry.getValue();

                final String cacheValue = cache.get(key);
                assertEquals(value, cacheValue);
            }
        }

        finally {
            // Shutdown kafka streamer.
            kafkaStmr.stop();
        }
    }

    /**
     * Creates default consumer config.
     *
     * @param zooKeeper Zookeeper address <server:port>.
     * @param groupId Group Id for kafka subscriber.
     * @return {@link ConsumerConfig} kafka consumer configuration.
     */
    private ConsumerConfig createDefaultConsumerConfig(String zooKeeper, String groupId) {
        A.notNull(zooKeeper, "zookeeper");
        A.notNull(groupId, "groupId");

        Properties props = new Properties();
        props.put("zookeeper.connect", zooKeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }

}
