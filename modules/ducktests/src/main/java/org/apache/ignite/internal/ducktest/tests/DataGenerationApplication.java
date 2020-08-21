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


    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        String cacheName = jsonNode.get("cacheName").asText();
        boolean infinite = jsonNode.hasNonNull("infinite") && jsonNode.get("infinite").asBoolean();
        int range = jsonNode.get("range").asInt();

        if (infinite) {
            boolean error = true;
            AtomicInteger cycle = new AtomicInteger();

            log.info("Generating data in background...");

            try {
                while (active()) {
                    generateData(cacheName, range, (idx) -> idx + cycle.get(), true, cycle.get() > 0);

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

            generateData(cacheName, range, Function.identity(), false, false);

            log.info("Data generation finished. Generated " + range + " entries.");

            markSyncExecutionComplete();
        }
    }

    /** */
    private void generateData(String cacheName, int range, Function<Integer, Integer> supplier, boolean markInited,
        boolean overwrite) {
        long notifyTime = System.nanoTime();
        int streamed = 0;

        if (log.isDebugEnabled())
            log.debug("Creating cache...");

        ignite.getOrCreateCache(cacheName);

        try (IgniteDataStreamer<Integer, Integer> streamer = ignite.dataStreamer(cacheName)) {
            streamer.allowOverwrite(overwrite);

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
