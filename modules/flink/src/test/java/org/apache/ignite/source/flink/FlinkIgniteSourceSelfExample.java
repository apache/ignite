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

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;

/**
 * Example usage for {@link IgniteSource}.
 */
public class FlinkIgniteSourceSelfExample {

    /**
     * Validation for the Flink source with EventCount and IgnitePredicate Filter. Ignite started in source based on
     * what is specified in the configuration file.
     */
    public static void main(String[] args) throws Exception {
        /** Cache name. */
        final String TEST_CACHE = "testCache";

        /** Grid Name. */
        final String GRID_NAME = "igniteServerNode";

        /** Ignite test configuration file. */
        final String GRID_CONF_FILE = "modules/flink/src/test/resources/example-ignite.xml";

        Ignite ignite = Ignition.start(GRID_CONF_FILE);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(TEST_CACHE);

        IgniteSource igniteSrc = new IgniteSource(TEST_CACHE);
        igniteSrc.setIgnite(ignite);
        igniteSrc.setEvtBatchSize(10);
        igniteSrc.setEvtBufTimeout(10);

        igniteSrc.start(null, EventType.EVT_CACHE_OBJECT_PUT);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().registerTypeWithKryoSerializer(CacheEvent.class, CacheEventSerializer.class);

        DataStream<CacheEvent> stream = env.addSource(igniteSrc);
        int cnt = 0;

        final List<Integer> eventList = new ArrayList<>();

        while (cnt < 10) {

            cache.put(cnt, cnt);

            eventList.add(cnt);

            cnt++;

        }
        stream.print();
        env.execute();
    }

}

