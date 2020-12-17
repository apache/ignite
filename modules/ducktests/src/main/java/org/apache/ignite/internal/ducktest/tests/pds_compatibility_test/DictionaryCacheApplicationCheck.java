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

package org.apache.ignite.internal.ducktest.tests.pds_compatibility_test;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

public class DictionaryCacheApplicationCheck extends IgniteAwareApplication {
    /**
     * {@inheritDoc}
     */
    @Override protected void run(JsonNode jsonNode) {
        log.info("Opening cache...");

        IgniteCache<Long, String> cache = ignite.cache(jsonNode.get("cacheName").asText());

        for (long i = 0; i < jsonNode.get("range").asLong(); i++) {
            cache.get(i);
        }
        log.info("Cache checked");
        markSyncExecutionComplete();
    }
}
