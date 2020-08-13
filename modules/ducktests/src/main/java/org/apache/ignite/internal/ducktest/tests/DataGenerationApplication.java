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
import java.util.Random;
import java.util.function.Function;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** */
    public static final String PARAM_RANGE = "range";

    /** */
    public static final String PARAM_INFINITE = "infinite";

    /** */
    public static final String PARAM_CACHE_NAME = "cacheName";

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_NANO = 1500 * 1000000L;

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_AMOUNT = 10_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        log.info("Creating cache...");

        IgniteCache<Integer, Integer> cache = ignite.createCache(jsonNode.get(PARAM_CACHE_NAME).asText());

        boolean infinite = jsonNode.hasNonNull(PARAM_INFINITE) && jsonNode.get(PARAM_INFINITE).asBoolean();

        log.warn("REMOVETHIS. infinite = " + infinite);
        log.warn("REMOVETHIS. jsonNode = " + jsonNode);

        int range = jsonNode.get(PARAM_RANGE).asInt();

        if (infinite) {
            Random rnd = new Random();

            Thread th = new Thread(() -> {
                log.info("Generating data in background...");

                try {
                    while (!stopped() && !Thread.interrupted())
                        generateData(cache, range, (idx) -> rnd.nextInt(range));

                    log.info("Background data generation finished.");
                }
                catch (Exception e) {
                    log.error("Failed to generate data in background.", e);
                }

                markFinished();
            }, DataGenerationApplication.class.getName());

            th.setDaemon(true);

            th.start();

            markInitialized();
        }
        else {
            log.info("Generating data...");

            try {
                generateData(cache, range, Function.identity());

                log.info("Data generation finished. Generated " + range + " entries.");
            }
            catch (Exception e) {
                log.error("Failed to generate data.", e);
            }

            markSyncExecutionComplete();
        }
    }

    /** */
    private void generateData(IgniteCache<Integer, Integer> cache, int range, Function<Integer, Integer> supplier) {
        long notifyTime = System.nanoTime();
        int streamed = 0;

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cache.getName())) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < range && !stopped() && Thread.interrupted(); i++) {
                streamer.addData(i, supplier.apply(i));

                if (notifyTime + DATAGEN_NOTIFY_INTERVAL_NANO < System.nanoTime() ||
                    i - streamed >= DATAGEN_NOTIFY_INTERVAL_AMOUNT) {
                    notifyTime = System.nanoTime();

                    log.warn("REMOVETHIS. Streamed " + (i - streamed) + " entries. Total: " + i + '.');

                    if (log.isDebugEnabled())
                        log.debug("Streamed " + (i - streamed) + " entries. Total: " + i + '.');

                    streamed = i;
                }
            }
        }
    }
}
