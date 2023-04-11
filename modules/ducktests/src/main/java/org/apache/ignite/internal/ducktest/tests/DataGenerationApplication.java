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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;

/**
 * Application generates cache data with data streamer by specified parameters.
 */
public class DataGenerationApplication extends AbstractDataLoadApplication {
    /** Max streamer data size. */
    private static final int MAX_STREAMER_DATA_SIZE = 100_000_000;

    /** Application config. */
    private DataGenerationConfig dataGenCfg;

    /** {@inheritDoc} */
    @Override protected void parseConfig(JsonNode jsonNode) {
        super.parseConfig(jsonNode);

        dataGenCfg = parseConfig(jsonNode, DataGenerationConfig.class);
    }

    /** {@inheritDoc} */
    @Override protected void loadData() {
        for (int i = 1; i <= dataGenCfg.cacheCount; i++)
            generateCacheData(cacheName(i));
    }

    /**
     * @param cacheName Cache name.
     */
    private void generateCacheData(String cacheName) {
        int flushEach = MAX_STREAMER_DATA_SIZE / dataGenCfg.entrySize + (MAX_STREAMER_DATA_SIZE % dataGenCfg.entrySize == 0 ? 0 : 1);
        int logEach = (dataGenCfg.to - dataGenCfg.from) / 10;

        try (IgniteDataStreamer<Integer, BinaryObject> stmr = ignite.dataStreamer(cacheName)) {
            for (int i = dataGenCfg.from; i < dataGenCfg.to; i++) {
                stmr.addData(i, cacheEntryValue(i));

                if ((i - dataGenCfg.from + 1) % logEach == 0 && log.isDebugEnabled())
                    log.debug("Streamed " + (i - dataGenCfg.from + 1) + " entries into " + cacheName);

                if (i % flushEach == 0)
                    stmr.flush();
            }
        }

        log.info(cacheName + " data generated [" +
            "entryCnt=" + (dataGenCfg.from - dataGenCfg.to) +
            ", from=" + dataGenCfg.from +
            ", to=" + dataGenCfg.to + "]");
    }

    /** Describes specific params for this generator. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class DataGenerationConfig {
        /** Start key to generate data. */
        private int from;

        /** Finish key to generate data. */
        private int to;

        /** Count of caches to fill with the data. */
        private int cacheCount;

        /** Test caches entry size. */
        private int entrySize;
    }
}
