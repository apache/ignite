/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.stream.rocketmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.stream.rocketmq.TestRocketMQServer.NAME_SERVER_PORT;
import static org.apache.ignite.stream.rocketmq.TestRocketMQServer.TEST_IP;

/**
 * Test for {@link RocketMQStreamer}.
 */
public class RocketMQStreamerTest extends GridCommonAbstractTest {
    /** Test topic. */
    private static final String TOPIC_NAME = "testTopic";

    /** Test consumer group. */
    private static final String CONSUMER_GRP = "testConsumerGrp";

    /** Test server. */
    private static TestRocketMQServer testRocketMQServer;

    /** Number of events to handle. */
    private static final int EVT_NUM = 1000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(DEFAULT_CACHE_NAME).clear();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        testRocketMQServer = new TestRocketMQServer(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (testRocketMQServer != null)
            testRocketMQServer.shutdown();
    }

    /** Constructor. */
    public RocketMQStreamerTest() {
        super(true);
    }

    /**
     * Tests data is properly injected into the grid.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testStreamer() throws Exception {
        RocketMQStreamer<String, byte[]> streamer = null;

        Ignite ignite = grid();

        try (IgniteDataStreamer<String, byte[]> dataStreamer = ignite.dataStreamer(DEFAULT_CACHE_NAME)) {
            dataStreamer.allowOverwrite(true);
            dataStreamer.autoFlushFrequency(10);

            streamer = new RocketMQStreamer<>();

            //configure.
            streamer.setIgnite(ignite);
            streamer.setStreamer(dataStreamer);
            streamer.setNameSrvAddr(TEST_IP + ":" + NAME_SERVER_PORT);
            streamer.setConsumerGrp(CONSUMER_GRP);
            streamer.setTopic(TOPIC_NAME);
            streamer.setMultipleTupleExtractor(new TestTupleExtractor());

            streamer.start();

            IgniteCache<String, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

            assertEquals(0, cache.size(CachePeekMode.PRIMARY));

            final CountDownLatch latch = new CountDownLatch(EVT_NUM);

            IgniteBiPredicate<UUID, CacheEvent> putLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    assert evt != null;

                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).remoteListen(putLsnr, null, EVT_CACHE_OBJECT_PUT);

            produceData();

            assertTrue(latch.await(30, TimeUnit.SECONDS));

            assertEquals(EVT_NUM, cache.size(CachePeekMode.PRIMARY));
        }
        finally {
            if (streamer != null)
                streamer.stop();
        }
    }

    /**
     * Test tuple extractor.
     */
    public static class TestTupleExtractor implements StreamMultipleTupleExtractor<List<MessageExt>, String, byte[]> {

        /** {@inheritDoc} */
        @Override public Map<String, byte[]> extract(List<MessageExt> msgs) {
            final Map<String, byte[]> map = new HashMap<>();

            for (MessageExt msg : msgs)
                map.put(msg.getMsgId(), msg.getBody());

            return map;
        }
    }

    /**
     * Adds data to RocketMQ.
     *
     * @throws Exception If fails.
     */
    private void produceData() throws Exception {
        initTopic(TOPIC_NAME, TEST_IP + ":" + NAME_SERVER_PORT);

        DefaultMQProducer producer = new DefaultMQProducer("testProducerGrp");

        producer.setNamesrvAddr(TEST_IP + ":" + NAME_SERVER_PORT);

        try {
            producer.start();

            for (int i = 0; i < EVT_NUM; i++)
                producer.send(new Message(TOPIC_NAME, "", String.valueOf(i).getBytes("UTF-8")));
        }
        catch (Exception e) {
            throw new Exception(e);
        }
        finally {
            producer.shutdown();
        }
    }

    /**
     * Initializes RocketMQ topic.
     *
     * @param topic Topic.
     * @param nsAddr Nameserver address.
     * @throws IgniteInterruptedCheckedException If fails.
     */
    private void initTopic(String topic, String nsAddr) throws Exception {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr(nsAddr);
        try {
            defaultMQAdminExt.start();

            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setTopicName(topic);
            topicConfig.setReadQueueNums(4);
            topicConfig.setWriteQueueNums(4);

            defaultMQAdminExt.createAndUpdateTopicConfig(testRocketMQServer.getBrokerAddr(), topicConfig);

            U.sleep(100);
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
