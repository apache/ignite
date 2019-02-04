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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link IgniteSink}.
 */
public class FlinkIgniteSinkSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String TEST_CACHE = "testCache";

    /** Ignite test configuration file. */
    private static final String GRID_CONF_FILE = "modules/flink/src/test/resources/example-ignite.xml";

    @Test
    public void testIgniteSink() throws Exception {
        Configuration configuration = new Configuration();

        IgniteSink igniteSink = new IgniteSink(TEST_CACHE, GRID_CONF_FILE);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(1L);

        igniteSink.open(configuration);

        Map<String, String> myData = new HashMap<>();
        myData.put("testData", "testValue");

        igniteSink.invoke(myData);

        /** waiting for a small duration for the cache flush to complete */
        Thread.sleep(2000);

        assertEquals("testValue", igniteSink.getIgnite().getOrCreateCache(TEST_CACHE).get("testData"));
    }

    @Test
    public void testIgniteSinkStreamExecution() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        IgniteSink igniteSink = new IgniteSink(TEST_CACHE, GRID_CONF_FILE);

        igniteSink.setAllowOverwrite(true);

        igniteSink.setAutoFlushFrequency(1);

        Map<String, String> myData = new HashMap<>();
        myData.put("testdata", "testValue");
        DataStream<Map> stream = env.fromElements(myData);

        stream.addSink(igniteSink);
        try {
            env.execute();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Stream execution process failed.");
        }
    }
}
