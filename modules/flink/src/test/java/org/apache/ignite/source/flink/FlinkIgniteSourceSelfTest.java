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

package org.apache.ignite.source.flink;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link IgniteSource}.
 */
public class FlinkIgniteSourceSelfTest extends GridCommonAbstractTest {

    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 10;

    /** Streaming events for testing. */
    private static final long DFLT_STREAMING_EVENT = 10;

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
     * Tests for the Flink source.
     * Ignite started in source based on what is specified in the configuration file.
     *
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void testFlinkIgniteSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().registerTypeWithKryoSerializer(CacheEvent.class, CacheEventSerializer.class);
        IgniteCache cache = ignite.cache(TEST_CACHE);

        IgniteSource igniteSource = new IgniteSource(TEST_CACHE, GRID_CONF_FILE);

        igniteSource.start(10, 10, 10, "PUT");

        DataStream<CacheEvent> stream = env.addSource(igniteSource);

        int cnt = 0;
        while (cnt < 10)  {
            cache.put(cnt, cnt);
            cnt++;
        }
        X.println(">>> Printing stream results.");
        stream.print();
        stream.addSink(new SinkFunction<CacheEvent>() {
            @Override
            public void invoke(CacheEvent cacheEvent) throws Exception {
                assertNotNull(cacheEvent.newValue().toString());
                assertTrue(Integer.parseInt(cacheEvent.newValue().toString()) < 10);
            }
        });
        try {
            env.execute();
        }
        finally {
            igniteSource.stop();
        }
    }
}
