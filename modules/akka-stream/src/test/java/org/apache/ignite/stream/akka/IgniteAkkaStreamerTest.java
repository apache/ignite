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
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.stream.scala.akka.IgniteAkkaActorJavaStreamer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link IgniteAkkaStreamer}.
 */
public class IgniteAkkaStreamerTest extends GridCommonAbstractTest {
    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 1000;

    /** Cache name. */
    private static final String CACHE_NAME = "akka";

    /** Actor system. */
    private final ActorSystem system = ActorSystem.create("AkkaStreamIgnite");

    /** Actor materializer. */
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
        grid().getOrCreateCache(new CacheConfiguration<>(CACHE_NAME));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        materializer.shutdown();
    }

    /**
     * @throws Exception Test exception.
     */
    public void testStreamer1() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(CACHE_NAME)) {
            IgniteAkkaStreamer streamer = newStreamerInstance(dataStreamer);
            executeStreamer(streamer.foreach());
        }
    }

    /**
     * @throws Exception Test exception.
     */
    public void testStreamer2() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(CACHE_NAME)) {
            IgniteAkkaStreamer streamer = newStreamerInstance(dataStreamer);
            executeStreamer(streamer.foreachParallel(5, system.dispatcher()));
        }
    }

    /**
     * @throws Exception Test exception. Extends StreamAdapter scala
     */
    public void testStreamer3() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(CACHE_NAME)) {
            executeActorStreamer(dataStreamer);
        }
    }

    /**
     * @throws Exception Test exception. Extends StreamAdapter java
     */
    public void testStreamer4() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(CACHE_NAME)) {
            executeActorStreamer(dataStreamer);
        }
    }

    /**
     * Execute akka streamer wrap akka-stream.
     */
    private void executeStreamer(Sink sink) throws Exception {
        IgniteCache<Integer, Integer> cache = grid().cache(CACHE_NAME);

        CacheListener listener = subscribeToPutEvents();

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        buildAkkaStreamer(sink);

        CountDownLatch latch = listener.getLatch();

        latch.await();

        unsubscribeToPutEvents(listener);

        assertTrue(cache.get(CACHE_ENTRY_COUNT - 1) instanceof Integer);

        assertTrue(cache.size(CachePeekMode.PRIMARY) == CACHE_ENTRY_COUNT);

        cache.clear();
    }

    /**
     * Execute akka streamer wrap akka-actor.
     */
    private void executeActorStreamer(IgniteDataStreamer<Integer, String> dataStreamer) throws Exception {
        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        final ActorRef actorRef = system.actorOf(Props.create(IgniteAkkaActorJavaStreamer.class, dataStreamer, new MySingleTupleExtractor(), null), "ignite-streamer");

        IgniteCache<Integer, Integer> cache = grid().cache(CACHE_NAME);

        CacheListener listener = subscribeToPutEvents();

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        for (int i = 0; i < CACHE_ENTRY_COUNT; i++) {
            actorRef.tell(new Integer(i), actorRef);
        }

        CountDownLatch latch = listener.getLatch();

        latch.await();

        unsubscribeToPutEvents(listener);

        assertTrue(cache.get(CACHE_ENTRY_COUNT - 1) instanceof Integer);

        assertTrue(cache.size(CachePeekMode.PRIMARY) == CACHE_ENTRY_COUNT);

        cache.clear();
    }

    /**
     * @return Akka-stream streamer.
     */
    private IgniteAkkaStreamer newStreamerInstance(IgniteDataStreamer<Integer, String> dataStreamer) {
        IgniteAkkaStreamer streamer = new IgniteAkkaStreamer();

        streamer.setSingleTupleExtractor(new MySingleTupleExtractor());

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /**
     * Build testing env for akka-stream as source, flow, sink.
     *
     * @param sink Sink object.
     */
    private void buildAkkaStreamer(Sink sink) {
        // Starting from a Source
        final Source<Integer, NotUsed> source = Source.from(getSourceData(CACHE_ENTRY_COUNT));

        final Flow<Integer, Integer, NotUsed> flow = Flow.of(Integer.class).map(elem -> elem + 20);

        RunnableGraph<NotUsed> rg = source.via(flow).to(sink);

        rg.run(materializer);
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

    /**
     * Data generator.
     *
     * @param size Size source list.
     * @return List data.
     */
    private List getSourceData(int size) {
        Integer[] list = new Integer[size];
        for (int i = 0; i < size; i++) {
            list[i] = (int)(Math.random() * 9 + size);
        }

        return Arrays.asList(list);
    }

    class MySingleTupleExtractor implements StreamSingleTupleExtractor<Integer,Integer,Integer> {
        final AtomicInteger count = new AtomicInteger(0);

        @Override public Map.Entry<Integer, Integer> extract(Integer msg) {
            return new IgniteBiTuple<>(count.getAndIncrement(), msg);
        }
    }
}
