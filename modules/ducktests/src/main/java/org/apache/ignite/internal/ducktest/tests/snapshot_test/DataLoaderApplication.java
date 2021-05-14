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

import java.util.Collections;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Loading data to cache.
 */
public class DataLoaderApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        String cacheName = jNode.get("cacheName").asText();
        long start = jNode.get("start").asLong();
        long interval = jNode.get("interval").asLong();
        int valSize = jNode.get("valueSizeKb").asInt() * 1024;

        markInitialized();

        QueryEntity qryEntity = new QueryEntity()
            .setKeyFieldName("id")
            .setKeyType(Long.class.getName())
            .setTableName("TEST_TABLE")
            .setValueType(byte[].class.getName())
            .addQueryField("id", Long.class.getName(), null)
            .setIndexes(Collections.singletonList(new QueryIndex("id")));

        CacheConfiguration<Long, byte[]> cacheCfg = new CacheConfiguration<>(cacheName);
        cacheCfg.setCacheMode(CacheMode.REPLICATED);
        cacheCfg.setQueryEntities(Collections.singletonList(qryEntity));

        ignite.getOrCreateCache(cacheCfg);

        byte[] data = new byte[valSize];

        try (IgniteDataStreamer<Long, byte[]> dataStreamer = ignite.dataStreamer(cacheName)) {
            for (long i = start; i < start + interval; i++)
                dataStreamer.addData(i, data);
        }

        markFinished();
    }
}
