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

package org.apache.ignite.internal.ducktest.tests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Deleting data from the cache.
 */
public class DeleteDataApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        String cacheName = jNode.get("cacheName").asText();

        int size = jNode.get("size").asInt();

        int batchSize = Optional.ofNullable(jNode.get("batchSize"))
            .map(JsonNode::asInt)
            .orElse(1000);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

        log.info("Cache size before: " + cache.size());

        markInitialized();

        long start = System.currentTimeMillis();

        Iterator<Cache.Entry<Object, Object>> iter = cache.iterator();

        ArrayList<Object> keys = new ArrayList<>(size);

        for (int cnt = 0; iter.hasNext() && cnt < size; cnt++)
            keys.add(iter.next().getKey());

        log.info("Start removing: " + keys.size());

        int listSize = keys.size();

        List<IgniteFuture<Void>> futures = new LinkedList<>();

        for (int from = 0; from < listSize; from += batchSize) {
            int to = Math.min(from + batchSize, listSize);

            futures.add(cache.removeAllAsync(new TreeSet<>(keys.subList(from, to))));
        }

        futures.forEach(f -> f.get(TimeUnit.MINUTES.toMillis(5)));

        log.info("Cache size after: " + cache.size());

        recordResult("DURATION_SECONDS", TimeUnit.MILLISECONDS.toSeconds( System.currentTimeMillis() - start));

        markFinished();
    }
}
