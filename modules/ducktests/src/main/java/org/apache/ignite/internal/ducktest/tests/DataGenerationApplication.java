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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        private String cacheName;
        /** */
        private boolean infinite;
        /** */
        private int range;
    }

    public static void main(String[] args){
        ObjectMapper objMapper = new ObjectMapper();
        objMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        String json = "{ \"cacheName\" : \"test-cache\", \"infinite\" : true, \"range\" : 100000 }";

        Config cfg = null;

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

        ignite.getOrCreateCache(cacheConfiguration);

        if (cfg.infinite) {
            boolean error = true;
            AtomicInteger cycle = new AtomicInteger();

            log.info("Generating data in background...");
            System.out.println("TEST : before data generation.");

            try {
                while (active()) {
                    generateData(cfg.cacheName, cfg.range, (idx) -> idx + cycle.get(), true, cycle.get() > 0);

                    cycle.incrementAndGet();
                }

                log.info("Background data generation finished.");

                error = false;
            }
            catch (Throwable e) {
                System.out.println("TEST : catch (Throwable e).");

                // The data streamer fails with an error on node stoppage event before the termination.
                if (X.hasCause(e, NodeStoppingException.class)) {
                    error = false;

                    System.out.println("TEST : catch (Throwable e), X.hasCause(e, NodeStoppingException.class).");
                } else if (e instanceof Exception) {
                    System.out.println("TEST : catch (Throwable e), has no NodeStoppingException.");

                    log.error("Failed to generate data in background.", e);

                    System.out.println("TEST : catch (Throwable e), has no NodeStoppingException, error logged.");
                }
            }
            finally {
                System.out.println("TEST : finally.");

                if (error) {
                    System.out.println("TEST : finally, error.");

                    markBroken();

                    System.out.println("TEST : finally, error, markBroken.");
                } else {
                    System.out.println("TEST : finally, not error.");

                    markFinished(false);

                    System.out.println("TEST : finally, not error, markFinished.");
                }
            }

            System.out.println("TEST : 9");
        }
        else {
            log.info("Generating data...");

            generateData(cfg.cacheName, cfg.range, Function.identity(), false, false);

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

    /** */
    private void generateData(String cacheName, int range, Function<Integer, Integer> supplier, boolean markInited,
        boolean overwrite) {
        long notifyTime = System.nanoTime();
        int streamed = 0;

        try (IgniteDataStreamer<Integer, Integer>  streamer = ignite.dataStreamer(cacheName)) {
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
