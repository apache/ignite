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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.lang.IgniteFuture;

import javax.cache.Cache;
import java.util.*;

/**
 * Loading random uuids in cache.
 */
public class DeleteDataApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        String cacheName = jNode.get("cacheName").asText();

        int iterSize = Optional.ofNullable(jNode.get("iterSize"))
                .map(JsonNode::asInt)
                .orElse(50);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

        log.info(">>> Cache size before: " + cache.size());

        markInitialized();

        long start = System.currentTimeMillis();

        Iterator<Cache.Entry<Object, Object>> iter = cache.iterator();

        ArrayList<Object> keys = new ArrayList<>(iterSize);

        int cnt = 0;

        while (iter.hasNext() && cnt < iterSize) {
            keys.add(iter.next().getKey());

            cnt++;
        }

        log.info(">>> Start removing: " + keys.size());

        int listSize = keys.size();
        int bachSize = 1000;

        int fromIdx = 0;
        int toIdx = 0;

        List<IgniteFuture<Void>> futures = new LinkedList<>();

        while(fromIdx < listSize) {
            toIdx = Math.min(fromIdx + bachSize, listSize);

            futures.add(cache.removeAllAsync(new TreeSet<>(keys.subList(fromIdx, toIdx))));

            fromIdx=toIdx+1;
        }

        futures.forEach(IgniteFuture::get);

        log.info(">>> Cache size after: " + cache.size());

        recordResult("DURATION", System.currentTimeMillis() - start);

        markFinished();
    }
}
