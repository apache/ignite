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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link IgniteSink}.
 */
public class FlinkIgniteSinkSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Ignite instance. */
    private Ignite ignite;

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "modules/flink/src/test/resources/example-ignite.xml";

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
    public void testFlinkIgniteSink() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();

        CollectionConfiguration colCfg = new CollectionConfiguration();

        colCfg.setCacheMode(CacheMode.PARTITIONED);

        IgniteSink igniteSink = new IgniteSink(TEST_CACHE, GRID_CONF_FILE, colCfg);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(1L);

        igniteSink.start();

        DataStream<Map> stream = env.addSource(new SourceFunction<Map>() {

            private boolean running = true;

            @Override
            public void run(SourceContext<Map> ctx) throws Exception {
                Map testDataMap = new HashMap<>();
                long cnt = 0;
                while (running && (cnt < 10))  {
                    testDataMap.put(cnt, "ignite-" + cnt);
                    cnt++;
                }
                ctx.collect(testDataMap);
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(1);

        assertEquals(0, ignite.getOrCreateCache(TEST_CACHE).size());

        // sink data into the grid.
        stream.addSink(igniteSink);

        env.execute();

        assertEquals(10, ignite.getOrCreateCache(TEST_CACHE).size());

        for(long i = 0;i < 10; i++){
            assertEquals("ignite-" + i, ignite.getOrCreateCache(TEST_CACHE).get(i));
        }

        igniteSink.stop();
    }
}
