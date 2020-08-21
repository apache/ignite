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
import org.apache.ignite.IgniteDataStreamer;
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
    private static final int DATAGEN_NOTIFY_INTERVAL_AMOUNT = 10_000;

    /** */
    private static final int DELAYED_INITIALIZATION_AMOUNT = 10_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        String cacheName = jsonNode.get("cacheName").asText();
        boolean infinite = jsonNode.hasNonNull("infinite") && jsonNode.get("infinite").asBoolean();
        boolean optimized = !jsonNode.hasNonNull("optimized") || jsonNode.get("optimized").asBoolean();
        int range = jsonNode.get("range").asInt();

        if (infinite) {
            log.info("Generating data in background...");

            while (active()) {
                generateData(cacheName, range, true, optimized, true);

                // Delayed initialization for small data amount ( < DELAYED_INITIALIZATION_AMOUNT ).
                if (!inited())
                    markInitialized();
            }

            log.info("Background data generation finished.");

            markFinished();
        }
        else {
            log.info("Generating data...");

            generateData(cacheName, range, false, optimized, false);

            log.info("Data generation finished. Generated " + range + " entries.");

            markSyncExecutionComplete();
        }
    }

    /** */
    private void generateData(String cacheName, int range, boolean delayedInit, boolean optimized, boolean overwrite) {
        long notifyTime = System.nanoTime();

        int streamed = 0;

        if (log.isDebugEnabled())
            log.debug("Creating cache...");

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(overwrite);

            for (int i = 0; i < range && active(); i++) {
                if (optimized)
                    streamer.addData(i, i);
                else
                    cache.put(i, i);

                if (notifyTime + DATAGEN_NOTIFY_INTERVAL_NANO < System.nanoTime() ||
                    i - streamed >= DATAGEN_NOTIFY_INTERVAL_AMOUNT) {
                    notifyTime = System.nanoTime();

                    if (log.isDebugEnabled())
                        log.debug("Streamed " + (i - streamed) + " entries. Total: " + i + '.');

                    streamed = i;
                }

                // Delayed notify of the initialization to make sure the data load has completelly began and
                // has produced some notable amount of data.
                if (delayedInit && !inited() && i >= DELAYED_INITIALIZATION_AMOUNT)
                    markInitialized();
            }

            if (log.isDebugEnabled())
                log.debug("Streamed " + range + " entries.");
        }
        catch (Exception e) {
            if (!X.hasCause(e, NodeStoppingException.class))
                throw e;
        }
    }
}
