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

package org.apache.ignite.sink.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Tests for {@link IgniteSink}.
 */
public class FlinkIgniteSinkSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 10000;

    /** Streaming events for testing. */
    private static final long DFLT_STREAMING_EVENT = 10000;

    /** Ignite instance. */
    private Ignite ignite;

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "modules/flink/src/test/resources/example-ignite.xml";

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = loadConfiguration(GRID_CONF_FILE);

        cfg.setClientMode(false);

        ignite = startGrid("igniteServerNode", cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Tests for the Flink sink.
     * Ignite started in sink based on what is specified in the configuration file.
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void testFlinkIgniteSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();

        IgniteSink igniteSink = new IgniteSink(TEST_CACHE, GRID_CONF_FILE);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(10);

        igniteSink.start();

        CacheListener listener = subscribeToPutEvents();

        DataStream<Map> stream = env.addSource(new SourceFunction<Map>() {

            private boolean running = true;

            @Override public void run(SourceContext<Map> ctx) throws Exception {
                Map testDataMap = new HashMap<>();
                long cnt = 0;

                while (running && (cnt < DFLT_STREAMING_EVENT))  {
                    testDataMap.put(cnt, "ignite-" + cnt);
                    cnt++;
                }

                ctx.collect(testDataMap);
            }

            @Override public void cancel() {
                running = false;
            }
        }).setParallelism(1);

        assertEquals(0, ignite.cache(TEST_CACHE).size());

        // sink data into the grid.
        stream.addSink(igniteSink);

        try {
            env.execute();

            CountDownLatch latch = listener.getLatch();

            // Enough events was handled in 10 seconds. Limited by test's timeout.
            latch.await();

            unsubscribeToPutEvents(listener);

            assertEquals(DFLT_STREAMING_EVENT, ignite.getOrCreateCache(TEST_CACHE).size());

            for (long i = 0; i < DFLT_STREAMING_EVENT; i++)
                assertEquals("ignite-" + i, ignite.getOrCreateCache(TEST_CACHE).get(i));

        }
        finally {
            igniteSink.stop();
        }
    }

    /**
     * Sets a listener for {@link EventType#EVT_CACHE_OBJECT_PUT}.
     *
     * @return Cache listener.
     */
    private CacheListener subscribeToPutEvents() {
        // Listen to cache PUT events and expect as many as messages as test data items.
        CacheListener listener = new CacheListener();

        ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).localListen(listener, EVT_CACHE_OBJECT_PUT);

        return listener;
    }

    /**
     * Removes the listener for {@link EventType#EVT_CACHE_OBJECT_PUT}.
     *
     * @param listener Cache listener.
     */
    private void unsubscribeToPutEvents(CacheListener listener) {
        ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).stopLocalListen(listener, EVT_CACHE_OBJECT_PUT);
    }

    /** Listener. */
    private class CacheListener implements IgnitePredicate<CacheEvent> {
        private final CountDownLatch latch = new CountDownLatch(CACHE_ENTRY_COUNT);

        /** @return Latch. */
        public CountDownLatch getLatch() {
            return latch;
        }

        /**
         * @param evt Cache Event.
         * @return {@code true}.
         */
        @Override public boolean apply(CacheEvent evt) {
            latch.countDown();

            return true;
        }
    }
}
