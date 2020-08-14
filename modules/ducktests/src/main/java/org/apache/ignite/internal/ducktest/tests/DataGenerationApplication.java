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
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
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
    private static final String PARAM_RANGE = "range";

    /** */
    private static final String PARAM_INFINITE = "infinite";

    /** */
    private static final String PARAM_CACHE_NAME = "cacheName";

    /** */
    private static final String PARAM_OPTIMIZED = "optimized";

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_NANO = 1500 * 1000000L;

    /** */
    private static final long DATAGEN_NOTIFY_INTERVAL_AMOUNT = 10_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        String cacheName = jsonNode.get(PARAM_CACHE_NAME).asText();
        boolean infinite = jsonNode.hasNonNull(PARAM_INFINITE) && jsonNode.get(PARAM_INFINITE).asBoolean();
        boolean optimized = !jsonNode.hasNonNull(PARAM_OPTIMIZED) || jsonNode.get(PARAM_OPTIMIZED).asBoolean();
        int range = jsonNode.get(PARAM_RANGE).asInt();

        if (infinite) {
            Random rnd = new Random();
            CountDownLatch exitLatch = new CountDownLatch(1);

            Thread th = new Thread(() -> {
                log.info("Begin generating data in background...");

                boolean error = false;

                try {
                    while (active())
                        generateData(cacheName, range, (idx) -> rnd.nextInt(range), optimized);

                    log.info("Background data generation finished.");
                }
                catch (Exception e) {
                    if (!X.hasCause(e, NodeStoppingException.class)) {
                        error = true;

                        log.error("Failed to generate data in background.", e);
                    }
                }
                finally {
                    if (!error)
                        markFinished();

                    exitLatch.countDown();
                }

            }, DataGenerationApplication.class.getName() + "_cacheLoader");

            th.start();

            markInitialized();

            try {
                exitLatch.await();
            }
            catch (InterruptedException e) {
                log.warn("Interrupted waiting for background loading.");
            }
        }
        else {
            log.info("Generating data...");

            generateData(cacheName, range, Function.identity(), optimized);

            log.info("Data generation finished. Generated " + range + " entries.");

            markSyncExecutionComplete();
        }
    }

    /** */
    private void generateData(String cacheName, int range, Function<Integer, Integer> supplier, boolean optimized) {
        long notifyTime = System.nanoTime();
        int streamed = 0;

        if(log.isDebugEnabled())
            log.debug("Creating cache...");

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(true);

            for (int i = 0; i < range && active(); i++) {
                if (optimized)
                    streamer.addData(i, supplier.apply(i));
                else
                    cache.put(i, supplier.apply(i));

                if (notifyTime + DATAGEN_NOTIFY_INTERVAL_NANO < System.nanoTime() ||
                    i - streamed >= DATAGEN_NOTIFY_INTERVAL_AMOUNT) {
                    notifyTime = System.nanoTime();

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
