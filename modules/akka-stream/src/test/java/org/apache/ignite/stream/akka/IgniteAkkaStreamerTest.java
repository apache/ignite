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

package org.apache.ignite.stream.akka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Tcp;
import akka.util.ByteString;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link IgniteAkkaStreamer}.
 */
public class IgniteAkkaStreamerTest extends GridCommonAbstractTest {
    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 4;

    /** */
    private static final String CACHE_NAME = null;

    /** */
    private final ActorSystem system = ActorSystem.create("AkkaStreamIgnite");

    private final ActorMaterializer materializer = ActorMaterializer.create(system);

    /** Constructor. */
    public IgniteAkkaStreamerTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        grid().getOrCreateCache(defaultCacheConfiguration());
    }

    /** */
    public void testExecuteStreamer() throws Exception {

        IgniteCache<Integer, Integer> cache = grid().cache(CACHE_NAME);

        CacheListener listener = subscribeToPutEvents();

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        // TODO impl
        akkaStreamer();

        CountDownLatch latch = listener.getLatch();

        latch.await();

        unsubscribeToPutEvents(listener);

        // Last element.
        int testId = CACHE_ENTRY_COUNT - 1;

        Integer cachedValue = cache.get(testId);

        System.out.println(cachedValue);

        // Akka message successfully put to cache.
        assertTrue(cachedValue.equals(24));

        assertTrue(cache.size() == CACHE_ENTRY_COUNT);

        cache.clear();

        materializer.shutdown();
    }

    /**
     * @return Akka Streamer.
     */
    private IgniteAkkaStreamer newAkkaStreamerInstance() {
        IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(CACHE_NAME);

        IgniteAkkaStreamer streamer = new IgniteAkkaStreamer();

        final AtomicInteger count = new AtomicInteger();
        count.set(0);

        streamer.setSingleTupleExtractor(new StreamSingleTupleExtractor<Integer, Integer, Integer>() {
            @Override public Map.Entry extract(Integer msg) {
                return new IgniteBiTuple<>(count.getAndIncrement(), msg);
            }
        });

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /** */
    private void akkaStreamer() {
        // Starting from a Source
        final Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4));

        final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class).map(elem -> elem + 20);

        RunnableGraph<NotUsed> r1 = source.via(flow).to(newAkkaStreamerInstance().sink());

        r1.run(materializer);
    }

    /**
     * @return Cache listener.
     */
    private CacheListener subscribeToPutEvents() {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items.
        CacheListener listener = new CacheListener();

        ignite.events(ignite.cluster().forCacheNodes(CACHE_NAME)).localListen(listener, EVT_CACHE_OBJECT_PUT);

        return listener;
    }

    /**
     * @param listener Cache listener.
     */
    private void unsubscribeToPutEvents(CacheListener listener) {
        Ignite ignite = grid();

        ignite.events(ignite.cluster().forCacheNodes(CACHE_NAME)).stopLocalListen(listener, EVT_CACHE_OBJECT_PUT);
    }

    /**
     * Listener.
     */
    private class CacheListener implements IgnitePredicate<CacheEvent> {

        /** */
        private final CountDownLatch latch = new CountDownLatch(CACHE_ENTRY_COUNT);

        /**
         * @return Latch.
         */
        public CountDownLatch getLatch() {
            return latch;
        }

        /**
         * @param evt Cache Event.
         * @return {@code true}.
         */
        @Override
        public boolean apply(CacheEvent evt) {
            latch.countDown();

            return true;
        }
    }
}
