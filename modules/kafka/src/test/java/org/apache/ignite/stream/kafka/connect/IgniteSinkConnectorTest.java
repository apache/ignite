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

package org.apache.ignite.stream.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.stream.kafka.TestKafkaBroker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.FutureCallback;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.mock;

/**
 * Tests for {@link IgniteSinkConnector}.
 */
public class IgniteSinkConnectorTest extends GridCommonAbstractTest {
    /** Number of input messages. */
    private static final int EVENT_CNT = 10000;

    /** Cache name. */
    private static final String CACHE_NAME = "testCache";

    /** Test topics. */
    private static final String[] TOPICS = {"test1", "test2"};

    /** Kafka partition. */
    private static final int PARTITIONS = 3;

    /** Kafka replication factor. */
    private static final int REPLICATION_FACTOR = 1;

    /** Test Kafka broker. */
    private TestKafkaBroker kafkaBroker;

    /** Worker to run tasks. */
    private Worker worker;

    /** Workers' herder. */
    private Herder herder;

    /** Ignite server node. */
    private Ignite grid;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = loadConfiguration("modules/kafka/src/test/resources/example-ignite.xml");

        cfg.setClientMode(false);

        grid = startGrid("igniteServerNode", cfg);

        kafkaBroker = new TestKafkaBroker();

        for (String topic : TOPICS)
            kafkaBroker.createTopic(topic, PARTITIONS, REPLICATION_FACTOR);

        WorkerConfig workerConfig = new StandaloneConfig(makeWorkerProps());

        OffsetBackingStore offsetBackingStore = mock(OffsetBackingStore.class);
        offsetBackingStore.configure(anyObject(Map.class));

        worker = new Worker(workerConfig, offsetBackingStore);
        worker.start();

        herder = new StandaloneHerder(worker);
        herder.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        herder.stop();

        worker.stop();

        kafkaBroker.shutdown();

        stopAllGrids();
    }

    /**
     * Tests the whole data flow from injecting data to Kafka to transferring it to the grid. It reads from two
     * specified Kafka topics, because a sink task can read from multiple topics.
     *
     * @throws Exception Thrown in case of the failure.
     */
    public void testSinkPuts() throws Exception {
        Map<String, String> sinkProps = makeSinkProps(Utils.join(TOPICS, ","));

        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
            @Override
            public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                if (error != null)
                    throw new RuntimeException("Failed to create a job!");
            }
        });

        herder.putConnectorConfig(
            sinkProps.get(ConnectorConfig.NAME_CONFIG),
            sinkProps, false, cb);

        cb.get();

        final CountDownLatch latch = new CountDownLatch(EVENT_CNT * TOPICS.length);

        final IgnitePredicate<Event> putLsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt != null;

                latch.countDown();

                return true;
            }
        };

        grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).localListen(putLsnr, EVT_CACHE_OBJECT_PUT);

        IgniteCache<String, String> cache = grid.cache(CACHE_NAME);

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        Map<String, String> keyValMap = new HashMap<>(EVENT_CNT * TOPICS.length);

        // Produces events for the specified number of topics
        for (String topic : TOPICS)
            keyValMap.putAll(produceStream(topic));

        // Checks all events successfully processed in 10 seconds.
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).stopLocalListen(putLsnr);

        // Checks that each event was processed properly.
        for (Map.Entry<String, String> entry : keyValMap.entrySet())
            assertEquals(entry.getValue(), cache.get(entry.getKey()));

        assertEquals(EVENT_CNT * TOPICS.length, cache.size(CachePeekMode.PRIMARY));
    }

    /**
     * Sends messages to Kafka.
     *
     * @param topic Topic name.
     * @return Map of key value messages.
     */
    private Map<String, String> produceStream(String topic) {
        List<KeyedMessage<String, String>> messages = new ArrayList<>(EVENT_CNT);

        Map<String, String> keyValMap = new HashMap<>();

        for (int evt = 0; evt < EVENT_CNT; evt++) {
            long runtime = System.currentTimeMillis();

            String key = topic + "_" + String.valueOf(evt);
            String msg = runtime + String.valueOf(evt);

            messages.add(new KeyedMessage<>(topic, key, msg));

            keyValMap.put(key, msg);
        }

        Producer<String, String> producer = kafkaBroker.sendMessages(messages);

        producer.close();

        return keyValMap;
    }

    /**
     * Creates properties for test sink connector.
     *
     * @param topics Topics.
     * @return Test sink connector properties.
     */
    private Map<String, String> makeSinkProps(String topics) {
        Map<String, String> props = new HashMap<>();

        props.put(ConnectorConfig.TOPICS_CONFIG, topics);
        props.put(ConnectorConfig.TASKS_MAX_CONFIG, "2");
        props.put(ConnectorConfig.NAME_CONFIG, "test-connector");
        props.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, IgniteSinkConnector.class.getName());
        props.put(IgniteSinkConstants.CACHE_NAME, "testCache");
        props.put(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE, "true");
        props.put(IgniteSinkConstants.CACHE_CFG_PATH, "example-ignite.xml");

        return props;
    }

    /**
     * Creates properties for Kafka Connect workers.
     *
     * @return Worker configurations.
     */
    private Map<String, String> makeWorkerProps() {
        Map<String, String> props = new HashMap<>();

        props.put(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put("internal.key.converter.schemas.enable", "false");
        props.put("internal.value.converter.schemas.enable", "false");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        props.put("key.converter.schemas.enable", "false");
        props.put("value.converter.schemas.enable", "false");
        props.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker.getBrokerAddress());
        // fast flushing for testing.
        props.put(WorkerConfig.OFFSET_COMMIT_INTERVAL_MS_CONFIG, "10");

        return props;
    }
}
