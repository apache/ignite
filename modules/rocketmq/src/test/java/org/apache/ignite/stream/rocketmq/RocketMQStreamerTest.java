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
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.test.base.IntegrationTestBase;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link RocketMQStreamer}.
 */
public class RocketMQStreamerTest extends GridCommonAbstractTest {
    /** Test topic. */
    private static final String TOPIC_NAME = "testTopic";

    /** Test consumer group. */
    private static final String CONSUMER_GRP = "testConsumerGrp";

    /** Test name server address. */
    private static final String NAMESRV_ADDR = "127.0.0.1:";

    private static NamesrvController nameSrv;

    private static BrokerController broker;

    /** Number of events to handle. */
    private static final int EVT_NUM = 1000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(null).clear();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTestsStarted() throws Exception {
        nameSrv = IntegrationTestBase.createAndStartNamesrv();

        broker = IntegrationTestBase.createAndStartBroker(
            NAMESRV_ADDR + nameSrv.getNettyServerConfig().getListenPort());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        broker.shutdown();

        nameSrv.shutdown();
    }

    /** Constructor. */
    public RocketMQStreamerTest() {
        super(true);
    }

    public void testStreamer() throws Exception {
        RocketMQStreamer<String, byte[]> streamer = null;

        Ignite ignite = grid();

        try (IgniteDataStreamer<String, byte[]> dataStreamer = ignite.dataStreamer(null)) {
            dataStreamer.allowOverwrite(true);
            dataStreamer.autoFlushFrequency(10);

            streamer = new RocketMQStreamer<>();

            //configure.
            streamer.setIgnite(ignite);
            streamer.setStreamer(dataStreamer);
            streamer.setNameSrvAddr(NAMESRV_ADDR + nameSrv.getNettyServerConfig().getListenPort());
            streamer.setConsumerGrp(CONSUMER_GRP);
            streamer.setTopic(TOPIC_NAME);
            streamer.setMultipleTupleExtractor(new TestTupleExtractor());

            streamer.start();

            IgniteCache<String, String> cache = ignite.cache(null);

            assertEquals(0, cache.size(CachePeekMode.PRIMARY));

            final CountDownLatch latch = new CountDownLatch(EVT_NUM);

            IgniteBiPredicate<UUID, CacheEvent> putLsnr = new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    assert evt != null;

                    latch.countDown();

                    return true;
                }
            };

            ignite.events(ignite.cluster().forCacheNodes(null)).remoteListen(putLsnr, null, EVT_CACHE_OBJECT_PUT);

            produceData();

            assertTrue(latch.await(30, TimeUnit.SECONDS));

            assertEquals(EVT_NUM, cache.size(CachePeekMode.PRIMARY));
        }
        finally {
            if (streamer != null)
                streamer.stop();
        }
    }

    public static class TestTupleExtractor implements StreamMultipleTupleExtractor<List<MessageExt>, String, byte[]> {

        @Override public Map<String, byte[]> extract(List<MessageExt> msgs) {
            final Map<String, byte[]> map = new HashMap<>();

            for (MessageExt msg : msgs) {
                map.put(msg.getMsgId(), msg.getBody());
            }

            return map;
        }
    }

    private void produceData() throws Exception {
        IntegrationTestBase.initTopic(
            TOPIC_NAME, NAMESRV_ADDR + nameSrv.getNettyServerConfig().getListenPort(),
            broker.getBrokerConfig().getBrokerClusterName());

        DefaultMQProducer producer = new DefaultMQProducer("testProducerGrp");
        producer.setNamesrvAddr(NAMESRV_ADDR + nameSrv.getNettyServerConfig().getListenPort());

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
}
