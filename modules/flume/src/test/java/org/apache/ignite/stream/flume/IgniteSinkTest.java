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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * {@link IgniteSink} test.
 */
public class IgniteSinkTest extends GridCommonAbstractTest {
    private static final int EVENT_CNT = 10000;
    private static final String CACHE_NAME = "testCache";
    private IgniteSink sink;
    private Context ctx;

    /** Constructor. */
    public IgniteSinkTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        grid().<String, Integer>getOrCreateCache(defaultCacheConfiguration().setName(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    public void testSink() throws Exception {
        initContext();

        Context channelContext = new Context();
        channelContext.put("capacity", String.valueOf(EVENT_CNT));
        channelContext.put("transactionCapacity", String.valueOf(EVENT_CNT));
        Channel memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, channelContext);

        final List<UUID> opId = new ArrayList<>(1);
        final CountDownLatch latch = new CountDownLatch(EVENT_CNT);
        final IgniteBiPredicate<UUID, CacheEvent> callback = new IgniteBiPredicate<UUID, CacheEvent>() {
            @Override public boolean apply(UUID uuid, CacheEvent evt) {
                latch.countDown();
                return true;
            }
        };

        sink = new IgniteSink(grid()) {
            // setting the listener on cache before sink processing starts
            @Override synchronized public void start() {
                super.start();
                opId.add(grid().events(grid().cluster().forCacheNodes(sink.getCacheName())).remoteListen(callback, null, EVT_CACHE_OBJECT_PUT));
            }
        };
        sink.setName("IgniteSink");

        Configurables.configure(sink, ctx);
        sink.setChannel(memoryChannel);
        sink.start();

        Transaction tx = memoryChannel.getTransaction();
        tx.begin();
        for (int i = 0; i < EVENT_CNT; i++) {
            Event event = EventBuilder.withBody((String.valueOf(i) + ": " + i).getBytes());
            memoryChannel.put(event);
        }
        tx.commit();
        tx.close();

        Sink.Status status = Sink.Status.READY;
        while (status != Sink.Status.BACKOFF) {
            status = sink.process();
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));

        // check with Ignite
        IgniteCache<String, Integer> cache = grid().cache(sink.getCacheName());
        for (int i = 0; i < EVENT_CNT; i++) {
            assertEquals(i, (int)cache.get(String.valueOf(i)));
        }
        assertEquals(EVENT_CNT, cache.size(CachePeekMode.PRIMARY));

        if (opId != null && opId.size() == 1)
            grid().events(grid().cluster().forCacheNodes(sink.getCacheName())).stopRemoteListen(opId.get(0));

        sink.stop();
    }

    /**
     * Flume sink's context needed to start a data streamer.
     */
    private void initContext() {
        ctx = new Context();
        ctx.put(IgniteSinkConstants.CFG_CACHE_NAME, CACHE_NAME);
        ctx.put(IgniteSinkConstants.CFG_PATH, "path/to/ignite.xml");
        ctx.put(IgniteSinkConstants.CFG_EVENT_TRANSFORMER, "org.apache.ignite.stream.flume.TestEventTransformer");
    }
}
