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

package org.apache.ignite.internal.ducktest.tests.snapshot_test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Loading random uuids to cache.
 */
public class DataLoaderApplication extends IgniteAwareApplication {
    /** */
    private static final int KB = 1 << 10;

    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        String cacheName = jNode.get("cacheName").asText();

        int interval = jNode.get("interval").asInt();

        int dataSize = jNode.get("dataSizeKB").asInt() * KB;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long start = rnd.nextLong();

        markInitialized();

        LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("id", "java.lang.Long");
        fields.put("name", "java.lang.String");

        QueryEntity queryEntity = new QueryEntity()
            .setKeyFieldName("id")
            .setKeyType(Long.class.getName())
            .setValueType(TestData.class.getName())
            .setFields(fields)
            .setIndexes(Arrays.asList(new QueryIndex("id"), new QueryIndex("name")));

        CacheConfiguration<Long, TestData> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(CacheMode.REPLICATED);
        cacheCfg.setQueryEntities(Collections.singleton(queryEntity));

        ignite.getOrCreateCache(cacheCfg);

        try (IgniteDataStreamer<Long, TestData> dataStreamer = ignite.dataStreamer(cacheName)) {
            dataStreamer.autoFlushFrequency(1000);

            for (long i = start; i < start + interval; i++) {

                dataStreamer.addData(i, new TestData(i,"data_" + i));
            }
        }

        markFinished();
    }

    /**
     * Test class for indexed types.
     */
    private static class TestData {
        /** */
        long id;

        /** */
        String name;
        /** */
        public TestData(long id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
