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
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link IgniteSource}.
 */
@SuppressWarnings("unchecked")
public class FlinkIgniteSourceSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Grid Name. */
    private static final String GRID_NAME = "igniteServerNode";

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

        cfg.setGridName(GRID_NAME);

        G.getOrStart(cfg);
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
    public void testFlinkIgniteSource() throws Exception {
        checkIgniteSource(1, 1, null);
    }

    /**
     * Tests for the Flink source with Integer Predicate Filter.
     * Ignite started in source based on what is specified in the configuration file.
     *
     * @throws Exception
     */
    public void testFlinkIgniteSourceWithFilter() throws Exception {
        checkIgniteSource(1, 1, new IgnitePredicateInteger());
    }

    /**
     * Tests for the Flink source for large batch.
     * Ignite started in source based on what is specified in the configuration file.
     *
     * @throws Exception
     */
    public void testFlinkIgniteSourceWithLargeBatch() throws Exception {
        checkIgniteSource(100, 1, null);
    }

    /**
     * Validation for the Flink source with EventCount and IgnitePredicate Filter.
     * Ignite started in source based on what is specified in the configuration file.
     *
     * @param evtCount event count to process.
     * @param parallelismCnt DataStreamSource parallelism count
     * @param filter IgnitePredicate filter to filter events.
     *
     * @throws Exception
     */
    private void checkIgniteSource(final int evtCount, int parallelismCnt, IgnitePredicate<CacheEvent> filter) throws Exception {
        Ignite ignite = G.ignite(GRID_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().disableSysoutLogging();
        env.getConfig().registerTypeWithKryoSerializer(CacheEvent.class, CacheEventSerializer.class);

        final IgniteSource igniteSrc = new IgniteSource(TEST_CACHE);

        igniteSrc.setIgnite(ignite);
        igniteSrc.setEvtBatchSize(1);
        igniteSrc.setEvtBufTimeout(10);

        igniteSrc.start(filter, EventType.EVT_CACHE_OBJECT_PUT);

        IgniteCache cache = ignite.getOrCreateCache(TEST_CACHE);

        DataStream<CacheEvent> stream = env.addSource(igniteSrc).setParallelism(parallelismCnt);

        int cnt = 0;

        final Map<Integer, Integer> eventMap = new HashMap<>();

        while (cnt < evtCount)  {
            cache.put(cnt, cnt);
            eventMap.put(cnt, cnt);

            cnt++;
        }

        X.println(">>> Printing stream results.");

        stream.print();

        stream.addSink(new SinkFunction<CacheEvent>() {
            int sinkEvtCntr = 0;

            @Override public void invoke(CacheEvent cacheEvt) throws Exception {
                sinkEvtCntr++;

                assertNotNull(cacheEvt.newValue().toString());
                Integer k = Integer.parseInt(cacheEvt.newValue().toString());
                assertTrue(eventMap.containsKey(k));

                if(sinkEvtCntr == evtCount)
                    igniteSrc.stop();
            }
        });

        env.execute();
        igniteSrc.stop();
    }
}


