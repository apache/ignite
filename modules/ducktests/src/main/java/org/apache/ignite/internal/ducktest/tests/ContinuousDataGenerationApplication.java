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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Application generates cache data with data streamer by specified parameters.
 */
public class ContinuousDataGenerationApplication extends AbstractDataLoadApplication {
    /** Application config. */
    private DataGenerationConfig dataGenCfg;

    /** {@inheritDoc} */
    @Override protected void parseConfig(JsonNode jsonNode) {
        super.parseConfig(jsonNode);

        dataGenCfg = parseConfig(jsonNode, DataGenerationConfig.class);
    }

    /** {@inheritDoc} */
    @Override protected void loadData() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long notifyTime = System.nanoTime();

        int loaded = 0;

        while (active()) {
            for (int i = 1; i <= dataGenCfg.cacheCount; i++) {
                int key = rnd.nextInt();

                ignite.cache(cacheName(i)).put(key, cacheEntryValue(key));

                if (notifyTime + TimeUnit.MILLISECONDS.toNanos(1500) < System.nanoTime()) {
                    log.info("Put " + loaded + " entries into " + cacheName(1));

                    notifyTime = System.nanoTime();
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void createCaches() {
        // No-op.
    }

    /** Describes specific params for this generator. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class DataGenerationConfig {
        /** Count of caches to fill with the data. */
        private int cacheCount;
    }
}
