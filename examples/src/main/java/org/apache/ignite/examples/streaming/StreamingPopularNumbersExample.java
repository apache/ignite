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

package org.apache.ignite.examples.streaming;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.expiry.*;
import javax.cache.processor.*;
import java.util.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Real time popular numbers counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link org.apache.ignite.examples.ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-compute.xml} configuration.
 */
public class StreamingPopularNumbersExample {
    /**
     * Cache name.
     */
    private static final String STREAM_NAME = StreamingPopularNumbersExample.class.getSimpleName();

    /**
     * Count of most popular numbers to retrieve from cluster.
     */
    private static final int POPULAR_NUMBERS_CNT = 10;

    /**
     * Random number generator.
     */
    private static final Random RAND = new Random();

    /**
     * Range within which to generate numbers.
     */
    private static final int RANGE = 1000;

    /**
     * Count of total numbers to generate.
     */
    private static final int CNT = 1000000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws org.apache.ignite.IgniteException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            System.out.println();
            System.out.println(">>> Cache popular numbers example started.");

            /*
             * Configure streaming cache.
             * =========================
             */
            CacheConfiguration<Integer, Long> cfg = new CacheConfiguration<>();

            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setName(STREAM_NAME);
            cfg.setIndexedTypes(Integer.class, Long.class);

            // Sliding window of 1 seconds.
            cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, 1))));

            /**
             * Start the streaming cache on all server nodes.
             * ============================================
             */
            try (IgniteCache<Integer, Long> stmCache = ignite.createCache(cfg)) {
                // Check that that server nodes have been started.
                if (ignite.cluster().forDataNodes(STREAM_NAME).nodes().isEmpty()) {
                    System.out.println("Ignite does not have streaming cache configured: " + STREAM_NAME);

                    return;
                }

                // Stream random numbers from another thread.
                Thread th = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(STREAM_NAME)) {
                            stmr.allowOverwrite(true); // Allow data updates.
                            stmr.updater(new IncrementingUpdater());

                            while (!Thread.currentThread().isInterrupted())
                                stmr.addData(RAND.nextInt(RANGE), 1L);
                        }
                    }
                });

                th.start();

                // Run this example for 2 minutes.
                long duration = 2 * 60 * 1000;

                long start = System.currentTimeMillis();

                while (System.currentTimeMillis() - start < duration) {
                    // Select top 10 words.
                    SqlFieldsQuery top10 = new SqlFieldsQuery(
                        "select _key, _val from Long order by _val desc, _key limit ?").setArgs(10);

                    List<List<?>> results = stmCache.queryFields(top10).getAll();

                    for (List<?> res : results)
                        System.out.println(res.get(0) + "=" + res.get(1));

                    System.out.println("----------------");

                    Thread.sleep(5000);
                }

                th.interrupt();
                th.join();
            }
            catch (CacheException e) {
                e.printStackTrace();

                System.out.println("Destroying cache for name '" + STREAM_NAME + "'. Please try again.");

                ignite.destroyCache(STREAM_NAME);
            }
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     *
     * @param ignite Ignite.
     * @throws org.apache.ignite.IgniteException If failed.
     */
    private static void streamData(final Ignite ignite) throws IgniteException {
        try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(STREAM_NAME)) {
            stmr.allowOverwrite(true); // Allow data updates.
            stmr.updater(new IncrementingUpdater());

            while (!Thread.currentThread().isInterrupted())
                stmr.addData(RAND.nextInt(RANGE), 1L);
        }
        catch (IgniteInterruptedException ignore) {}
    }

    /**
     * Increments value for key.
     */
    private static class IncrementingUpdater implements IgniteDataStreamer.Updater<Integer, Long> {
        @Override
        public void update(IgniteCache<Integer, Long> cache, Collection<Map.Entry<Integer, Long>> entries) {
            for (Map.Entry<Integer, Long> entry : entries) {
                // Increment values by 1.
                cache.invoke(entry.getKey(), new EntryProcessor<Integer, Long, Object>() {
                    @Override
                    public Object process(MutableEntry<Integer, Long> e, Object... args) {
                        Long val = e.getValue();

                        e.setValue(val == null ? 1L : val + 1);

                        return null;
                    }
                });
            }
        }
    }
}
