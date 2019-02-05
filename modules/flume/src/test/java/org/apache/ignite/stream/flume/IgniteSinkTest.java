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

package org.apache.ignite.stream.flume;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * {@link IgniteSink} test.
 */
public class IgniteSinkTest extends GridCommonAbstractTest {
    /** Number of events to be sent to memory channel. */
    private static final int EVENT_CNT = 10000;

    /** Cache name. */
    private static final String CACHE_NAME = "testCache";

    /**
     * @throws Exception {@link Exception}.
     */
    @Test
    public void testSink() throws Exception {
        IgniteConfiguration cfg = loadConfiguration("modules/flume/src/test/resources/example-ignite.xml");

        cfg.setClientMode(false);

        final Ignite grid = startGrid("igniteServerNode", cfg);

        Context channelContext = new Context();

        channelContext.put("capacity", String.valueOf(EVENT_CNT));
        channelContext.put("transactionCapacity", String.valueOf(EVENT_CNT));

        Channel memoryChannel = new MemoryChannel();

        Configurables.configure(memoryChannel, channelContext);

        final CountDownLatch latch = new CountDownLatch(EVENT_CNT);

        final IgnitePredicate<Event> putLsnr = new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt != null;

                latch.countDown();

                return true;
            }
        };

        IgniteSink sink = new IgniteSink() {
            // Setting the listener on cache before sink processing starts.
            @Override public synchronized void start() {
                super.start();

                grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).localListen(putLsnr, EVT_CACHE_OBJECT_PUT);
            }
        };

        sink.setName("IgniteSink");
        sink.setChannel(memoryChannel);

        Context ctx = new Context();

        ctx.put(IgniteSinkConstants.CFG_CACHE_NAME, CACHE_NAME);
        ctx.put(IgniteSinkConstants.CFG_PATH, "example-ignite.xml");
        ctx.put(IgniteSinkConstants.CFG_EVENT_TRANSFORMER, "org.apache.ignite.stream.flume.TestEventTransformer");

        Configurables.configure(sink, ctx);

        sink.start();

        try {
            Transaction tx = memoryChannel.getTransaction();

            tx.begin();

            for (int i = 0; i < EVENT_CNT; i++)
                memoryChannel.put(EventBuilder.withBody((String.valueOf(i) + ": " + i).getBytes()));

            tx.commit();
            tx.close();

            Sink.Status status = Sink.Status.READY;

            while (status != Sink.Status.BACKOFF) {
                status = sink.process();
            }
        }
        finally {
            sink.stop();
        }

        // Checks that 10000 events successfully processed in 10 seconds.
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        grid.events(grid.cluster().forCacheNodes(CACHE_NAME)).stopLocalListen(putLsnr);

        IgniteCache<String, Integer> cache = grid.cache(CACHE_NAME);

        // Checks that each event was processed properly.
        for (int i = 0; i < EVENT_CNT; i++) {
            assertEquals(i, (int)cache.get(String.valueOf(i)));
        }

        assertEquals(EVENT_CNT, cache.size(CachePeekMode.PRIMARY));
    }
}
