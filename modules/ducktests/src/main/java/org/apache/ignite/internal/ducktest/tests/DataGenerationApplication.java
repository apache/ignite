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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Logger. */
    protected static final Logger log = LogManager.getLogger(DataGenerationApplication.class.getName());

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_NANO = 1500 * 1000000L;

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_AMOUNT = 10_000;

    /**
     * Configuration holder.
     */
    private static class Config {
        /** */
        private final String cacheName;
        /** */
        private final boolean infinite;
        /** */
        private final int range;

        /** */
        private Config(JsonNode jsonNode) {
            cacheName = jsonNode.get("cacheName").asText();
            infinite = jsonNode.hasNonNull("infinite") && jsonNode.get("infinite").asBoolean();
            range = jsonNode.get("range").asInt();
        }
    }


    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        Config cfg = new Config(jsonNode);

        CacheConfiguration<Integer, Integer> cacheConfiguration = new CacheConfiguration<>(cfg.cacheName);

        if (log.isDebugEnabled())
            log.debug("Creating cache, cache config: " + cacheConfiguration);

        log.warn("Creating cache, cache config: " + cacheConfiguration);

        ignite.getOrCreateCache(cacheConfiguration);

        if (cfg.infinite) {
            boolean error = true;
            AtomicInteger cycle = new AtomicInteger();

            log.info("Generating data in background...");

            try {
                while (active()) {
                    generateData(cfg.cacheName, cfg.range, (idx) -> idx + cycle.get(), true);

                    cycle.incrementAndGet();
                }

                log.info("Background data generation finished.");

                error = false;
            }
            catch (Throwable e) {
                // The data streamer fails with an error on node stoppage event before the termination.
                if (X.hasCause(e, NodeStoppingException.class))
                    error = false;
                else if (e instanceof Exception)
                    log.error("Failed to generate data in background.", e);
            }
            finally {
                if (error)
                    markBroken();
                else
                    markFinished(false);
            }
        }
        else {
            log.info("Generating data...");

            generateData(cfg.cacheName, cfg.range, Function.identity(), false);

            log.info("Data generation finished. Generated " + cfg.range + " entries.");

            markSyncExecutionComplete();
        }
    }

    /** */
    private void generateData(String cacheName, int range, Function<Integer, Integer> supplier, boolean markInited) {
        long notifyTime = System.nanoTime();
        int streamed = 0;

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < range && active(); i++) {
                streamer.addData(i, supplier.apply(i));

                if (notifyTime + DATAGEN_NOTIFY_INTERVAL_NANO < System.nanoTime() ||
                    i - streamed >= DATAGEN_NOTIFY_INTERVAL_AMOUNT) {
                    notifyTime = System.nanoTime();

                    if (markInited && !inited())
                        markInitialized();

                    if (log.isDebugEnabled())
                        log.debug("Streamed " + (i - streamed) + " entries. Total: " + i + '.');

                    streamed = i;
                }
            }

            if (log.isDebugEnabled())
                log.debug("Streamed " + range + " entries.");
        }
    }
}
