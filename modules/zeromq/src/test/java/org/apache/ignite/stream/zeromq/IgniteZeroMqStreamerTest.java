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

package org.apache.ignite.stream.zeromq;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.zeromq.ZMQ;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests {@link IgniteZeroMqStreamer}.
 */
public class IgniteZeroMqStreamerTest extends GridCommonAbstractTest {
    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 1000;

    /** Local address for 0mq. */
    private final String ADDR = "tcp://localhost:5671";

    /** Topic name for PUB-SUB. */
    private final byte[] TOPIC = "0mq".getBytes();

    /** Constructor. */
    public IgniteZeroMqStreamerTest() {
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

    /** {@inheritDoc} */
    @Override public void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception Test exception.
     */
    public void testZeroMqPairSocket() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(null)) {
            try (IgniteZeroMqStreamer streamer = newStreamerInstance(
                dataStreamer, 1, ZeroMqTypeSocket.PAIR, ADDR, null);) {
                executeStreamer(streamer, ZMQ.PAIR, null);
            }
        }
    }

    /**
     * @throws Exception Test exception.
     */
    public void testZeroMqSubSocket() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(null)) {
            try (IgniteZeroMqStreamer streamer = newStreamerInstance(
                dataStreamer, 3, ZeroMqTypeSocket.SUB, ADDR, TOPIC);) {
                executeStreamer(streamer, ZMQ.PUB, TOPIC);
            }
        }
    }

    /**
     * @throws Exception Test exception.
     */
    public void testZeroMqPullSocket() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(null)) {
            try (IgniteZeroMqStreamer streamer = newStreamerInstance(
                dataStreamer, 4, ZeroMqTypeSocket.PULL, ADDR, null);) {
                executeStreamer(streamer, ZMQ.PUSH, null);
            }
        }
    }

    /**
     * Execute ZeroMQ streamer and checking cache content after streaming finished.
     * Set singleTupleExtractor via {@link ZeroMqStringSingleTupleExtractor} in streamer.
     *
     * @param streamer ZeroMQ streamer.
     * @param clientSocket ZeroMQ socket type.
     * @param topic Topic name for PUB-SUB.
     * @throws Exception Test exception.
     */
    private void executeStreamer(IgniteZeroMqStreamer streamer, int clientSocket,
        byte[] topic) throws Exception {
        streamer.setSingleTupleExtractor(new ZeroMqStringSingleTupleExtractor());

        IgniteCache<Integer, String> cache = grid().cache(null);

        CacheListener listener = subscribeToPutEvents();

        streamer.start();

        assertEquals(0, cache.size(CachePeekMode.PRIMARY));

        startZeroMqClient(clientSocket, topic);

        CountDownLatch latch = listener.getLatch();

        latch.await();

        unsubscribeToPutEvents(listener);

        // Last element.
        int testId = CACHE_ENTRY_COUNT - 1;

        String cachedValue = cache.get(testId);

        // ZeroMQ message successfully put to cache.
        assertTrue(cachedValue != null && cachedValue.equals(String.valueOf(testId)));

        assertTrue(cache.size() == CACHE_ENTRY_COUNT);

        cache.clear();
    }

    /**
     * @param dataStreamer Ignite Data Streamer.
     * @return ZeroMQ Streamer.
     */
    private IgniteZeroMqStreamer newStreamerInstance(IgniteDataStreamer<Integer, String> dataStreamer,
        int ioThreads, ZeroMqTypeSocket socketType, @NotNull String addr, byte[] topic) {
        IgniteZeroMqStreamer streamer = new IgniteZeroMqStreamer(ioThreads, socketType, addr, topic);

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /**
     * Starts ZeroMQ client for testing.
     *
     * @param clientSocket ZeroMQ socket type.
     * @param topic Topic name for PUB-SUB.
     */
    private void startZeroMqClient(int clientSocket, byte[] topic) throws InterruptedException, UnsupportedEncodingException {
        try (ZMQ.Context context = ZMQ.context(1);
             ZMQ.Socket socket = context.socket(clientSocket)) {

            socket.bind(ADDR);

            if (ZMQ.PUB == clientSocket)
                Thread.sleep(500);

            for (int i = 0; i < CACHE_ENTRY_COUNT; i++) {
                if (ZMQ.PUB == clientSocket)
                    socket.sendMore(topic);
                socket.send(String.valueOf(i).getBytes("UTF-8"));
            }
        }
    }

    /**
     * @return Cache listener.
     */
    private CacheListener subscribeToPutEvents() {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items.
        CacheListener listener = new CacheListener();

        ignite.events(ignite.cluster().forCacheNodes(null)).localListen(listener, EVT_CACHE_OBJECT_PUT);

        return listener;
    }

    /**
     * @param listener Cache listener.
     */
    private void unsubscribeToPutEvents(CacheListener listener) {
        Ignite ignite = grid();

        ignite.events(ignite.cluster().forCacheNodes(null)).stopLocalListen(listener, EVT_CACHE_OBJECT_PUT);
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
