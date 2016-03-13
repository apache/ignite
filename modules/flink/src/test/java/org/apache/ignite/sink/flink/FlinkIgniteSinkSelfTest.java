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

import java.util.concurrent.TimeoutException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.hadoop.shaded.org.jboss.netty.util.internal.SystemPropertyUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for {@link IgniteSink}.
 */
public class FlinkIgniteSinkSelfTest extends GridCommonAbstractTest {

    /** Logger. */
    private IgniteLogger log;

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
     * Tests for the Flink sink. Ignite started in sink based on what is specified in the configuration file.
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    public void testFlinkIgniteSink() throws TimeoutException, InterruptedException {

        TypeInformation<Tuple2<Long, String>> longStringInfo = TypeInfoParser.parse("Tuple2<Long, String>");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();

        TypeInformationSerializationSchema<Tuple2<Long, String>> serSchema =
                new TypeInformationSerializationSchema<>(longStringInfo, env.getConfig());
        CollectionConfiguration colCfg = new CollectionConfiguration();
        colCfg.setCacheMode(CacheMode.PARTITIONED);
        IgniteSink igniteSink = new IgniteSink("myQueue", GRID_CONF_FILE, serSchema, colCfg);

        DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {

            private boolean running = true;

            @Override
            public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
                long cnt = 0;
                while (cnt < 100) {
                    ctx.collect(new Tuple2<Long, String>(cnt, "ignite-" + cnt));
                    cnt++;
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(1);

        // sink data into
        stream.addSink(igniteSink);

        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }

        assertTrue(igniteSink.getQueue().size()>0);
        ignite.log().info("Ignite Sink has "+ igniteSink.getQueue().size()+" elements");
    }
}
