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

import java.io.IOException;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Logger. */
    private static final Logger log = LogManager.getLogger(DataGenerationApplication.class.getName());

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL = 1500 * 1000000L;

    /** */
    private static final int DATAGEN_NOTIFY_INTERVAL_COUNT = 10_000;

    /** */
    private static final int WARMUP_DATA_PERCENT = 10;

    /** */
    private volatile boolean infinite;
    /**
     * Configuration holder.
     */
    private static class Config {
        /** */
        private String cacheName;

        /** */
        private boolean infinite;

        /** */
        private int range;

        /** */
        private Set<String> interrestingNodes;

        /** */
        private boolean transactional;
    }

    /** */
    public static void main(String[] args){
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        String json = "{ \"cacheName\" : \"test-cache\", \"infinite\" : true, \"range\" : 100000, " +
            "\"optimized\" : false, \"interrestingNodes\" : [\"sdfsd\", \"sdf5gj\", \"ask4j6\"] }";

        Config cfg;

        try {
            cfg = objMapper.readValue(json, Config.class);
            System.out.println("Cfg: " +  cfg);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        Config cfg = parseConfig(jsonNode);

        CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>(cfg.cacheName);

        if (log.isDebugEnabled())
            log.debug("Creating cache, cache config: " + cacheConfiguration);

        log.warn("Creating cache, cache config: " + cacheConfiguration);

        if (cfg.infinite) {
            log.info("Generating data in background...");

            while (active())
                generateData(cfg.cacheName, cfg.range, true, true);

            log.info("Background data generation finished.");
        }
        else {
            log.info("Generating data...");

            generateData(cfg.cacheName, cfg.range, false, false);

            log.info("Data generation finished. Generated " + cfg.range + " entries.");

            markSyncExecutionComplete();
        }
    }

    /** */
    private Config parseConfig(JsonNode node) {
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        Config cfg;

        log.warn("Parsing config: " + node.asText());

        try {
            cfg = objMapper.readValue(node.asText(), Config.class);
        }
        catch (Exception e) {
            throw new IllegalStateException("Unable to parse config.", e);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void stop() {
        super.stop();

        if (infinite)
            markFinished();
    }

    /** */
    private void generateData(String cacheName, int range, boolean warmUp, boolean overwrite) {
        long notifyTime = System.nanoTime();

        int streamed = 0;

        int warmUpCnt = (int)Math.max(1, (float)WARMUP_DATA_PERCENT / 100 * range);

        if (log.isDebugEnabled())
            log.debug("Creating cache...");

        ignite.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(overwrite);

            for (int i = 0; i < range && active(); i++) {
                streamer.addData(i, i);

                if (notifyTime + DATAGEN_NOTIFY_INTERVAL < System.nanoTime() ||
                    i - streamed >= DATAGEN_NOTIFY_INTERVAL_COUNT) {
                    notifyTime = System.nanoTime();

                    if (log.isDebugEnabled())
                        log.debug("Streamed " + (i - streamed) + " entries. Total: " + i + '.');

                    streamed = i;
                }

                // Delayed notify of the initialization to make sure the data load has completelly began and
                // has produced some notable amount of data.
                if (warmUp && !inited() && warmUpCnt == i + 1)
                    markInitialized();
            }

            if (log.isDebugEnabled())
                log.debug("Streamed " + range + " entries.");
        }
    }
}
