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

import java.util.Optional;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Loading random uuids in cache.
 */
public class UuidStreamerApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        IgniteCache<UUID, UUID> cache = ignite.getOrCreateCache(jNode.get("cacheName").asText());

        long size = Optional.ofNullable(jNode.get("size"))
                .map(JsonNode::asLong)
                .orElse(-1L);

        long cnt = 0L;

        long start = System.currentTimeMillis();

        markInitialized();

        while (!terminated() && cnt != size) {
            UUID uuid = UUID.randomUUID();

            cache.put(uuid, uuid);

            cnt++;
        }

        recordResult("STREAMED", cnt);
        recordResult("DURATION", System.currentTimeMillis() - start);

        markFinished();
    }
}
