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

import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.zeromq.ZMQ;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 *
 */
public class IgniteZeroMqStreamerTest extends GridCommonAbstractTest {
    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 10_000;

    /**  */
    private final String ADDR = "tcp://localhost:5671";

    /**  */
    private final byte[] TOPIC = "0mq".getBytes();

    /**  */
    public IgniteZeroMqStreamerTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20_000;
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() throws Exception {
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
            ZeroMqSettings zeroMqSettings = new ZeroMqSettings(1, ZeroMqTypeSocket.PAIR, ADDR, null);

            try (IgniteZeroMqStreamer streamer = newStreamerInstance(dataStreamer, zeroMqSettings);) {
                executeStreamer(streamer, ZMQ.PAIR, null);
            }
        }
    }

    /**
     * @throws Exception Test exception.
     */
    public void testZeroMqSubSocket() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(null)) {
            ZeroMqSettings zeroMqSettings = new ZeroMqSettings(1, ZeroMqTypeSocket.SUB, ADDR, TOPIC);

            try (IgniteZeroMqStreamer streamer = newStreamerInstance(dataStreamer, zeroMqSettings);) {
                executeStreamer(streamer, ZMQ.PUB, TOPIC);
            }
        }
    }

    /**
     * @throws Exception Test exception.
     */
    public void testZeroMqPullSocket() throws Exception {
        try (IgniteDataStreamer<Integer, String> dataStreamer = grid().dataStreamer(null)) {
            ZeroMqSettings zeroMqSettings = new ZeroMqSettings(1, ZeroMqTypeSocket.PULL, ADDR, null);

            try (IgniteZeroMqStreamer streamer = newStreamerInstance(dataStreamer, zeroMqSettings);) {
                executeStreamer(streamer, ZMQ.PUSH, null);
            }
        }
    }

    /**
     * @param streamer ZeroMQ streamer.
     * @param clientSocket .
     * @param topic .
     * @throws InterruptedException Test exception.
     */
    private void executeStreamer(IgniteZeroMqStreamer streamer, int clientSocket,
        byte[] topic) throws InterruptedException {
        streamer.setSingleTupleExtractor(new ZeroMqStringSingleTupleExtractor());

        // Checking streaming.
        CacheListener listener = subscribeToPutEvents();

        streamer.start();

        startZeroMqClient(clientSocket, topic);

        CountDownLatch latch = listener.getLatch();

        latch.await();

        unsubscribeToPutEvents(listener);

        // Checking cache content after streaming finished.
        IgniteCache<Integer, String> cache = grid().cache(null);

        int testId = CACHE_ENTRY_COUNT - 1;

        String cachedValue = cache.get(testId);

        // ZeroMQ message successfully put to cache.
        assertTrue(cachedValue != null && cachedValue.equals(String.valueOf(testId)));

        assertTrue(cache.size() == CACHE_ENTRY_COUNT);

        cache.clear();
    }

    /**
     * @param dataStreamer Ignite Data Streamer.
     * @returnd ZeroMQ Streamer.
     */
    private IgniteZeroMqStreamer newStreamerInstance(IgniteDataStreamer<Integer, String> dataStreamer,
        ZeroMqSettings zeroMqSettings) {
        IgniteZeroMqStreamer streamer = new IgniteZeroMqStreamer(zeroMqSettings);

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /**
     * Start ZeroMQ client for testing.
     *
     * @param clientSocket
     * @param topic
     */
    private void startZeroMqClient(int clientSocket, byte[] topic) throws InterruptedException {
        try (ZMQ.Context context = ZMQ.context(1);
             ZMQ.Socket socket = context.socket(clientSocket)) {

            socket.bind(ADDR);

            if (ZMQ.PUB == clientSocket)
                Thread.sleep(500);

            for (int i = 0; i < CACHE_ENTRY_COUNT; i++) {
                if (ZMQ.PUB == clientSocket)
                    socket.sendMore(topic);
                socket.send(String.valueOf(i).getBytes());
            }

            Thread.sleep(2000);
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
