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

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;

import javax.cache.processor.*;
import java.util.*;

import static org.apache.ignite.cache.query.Query.*;

/**
 * Real time popular numbers counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-cache.xml'}.
 * <p>
 * Alternatively you can run {@link CacheNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-cache.xml} configuration.
 */
public class CachePopularNumbersExample {
    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Count of most popular numbers to retrieve from cluster. */
    private static final int POPULAR_NUMBERS_CNT = 10;

    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

    /** Count of total numbers to generate. */
    private static final int CNT = 1000000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        Timer popularNumbersQryTimer = new Timer("numbers-query-worker");

        try (Ignite ignite = Ignition.start("examples/config/example-cache.xml")) {
            System.out.println();
            System.out.println(">>> Cache popular numbers example started.");

            // Clean up caches on all nodes before run.
            ignite.jcache(CACHE_NAME).clear();

            ClusterGroup prj = ignite.cluster().forCacheNodes(CACHE_NAME);

            if (prj.nodes().isEmpty()) {
                System.out.println("Ignite does not have cache configured: " + CACHE_NAME);

                return;
            }

            TimerTask task = scheduleQuery(ignite, popularNumbersQryTimer, POPULAR_NUMBERS_CNT);

            streamData(ignite);

            // Force one more run to get final counts.
            task.run();

            popularNumbersQryTimer.cancel();
        }
    }

    /**
     * Populates cache in real time with numbers and keeps count for every number.
     *
     * @param ignite Ignite.
     * @throws IgniteException If failed.
     */
    private static void streamData(final Ignite ignite) throws IgniteException {
        try (IgniteDataLoader<Integer, Long> ldr = ignite.dataLoader(CACHE_NAME)) {
            // Set larger per-node buffer size since our state is relatively small.
            ldr.perNodeBufferSize(2048);

            ldr.updater(new IncrementingUpdater());

            for (int i = 0; i < CNT; i++)
                ldr.addData(RAND.nextInt(RANGE), 1L);
        }
    }

    /**
     * Schedules our popular numbers query to run every 3 seconds.
     *
     * @param ignite Ignite.
     * @param timer Timer.
     * @param cnt Number of popular numbers to return.
     * @return Scheduled task.
     */
    private static TimerTask scheduleQuery(final Ignite ignite, Timer timer, final int cnt) {
        TimerTask task = new TimerTask() {
            @Override public void run() {
                // Get reference to cache.
                IgniteCache<Integer, Long> cache = ignite.jcache(CACHE_NAME);

                try {
                    List<List<?>> results = new ArrayList<>(cache.queryFields(
                        sql("select _key, _val from Long order by _val desc, _key limit ?").setArgs(cnt)).getAll());

                    for (List<?> res : results)
                        System.out.println(res.get(0) + "=" + res.get(1));

                    System.out.println("----------------");
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        timer.schedule(task, 3000, 3000);

        return task;
    }

    /**
     * Increments value for key.
     */
    private static class IncrementingUpdater implements IgniteDataLoader.Updater<Integer, Long> {
        /** Process entries to increase value by entry key. */
        private static final EntryProcessor<Integer, Long, Void> INC = new EntryProcessor<Integer, Long, Void>() {
            @Override public Void process(MutableEntry<Integer, Long> e, Object... args) {
                Long val = e.getValue();

                e.setValue(val == null ? 1L : val + 1);

                return null;
            }
        };

        /** {@inheritDoc} */
        @Override public void update(IgniteCache<Integer, Long> cache, Collection<Map.Entry<Integer, Long>> entries) {
            for (Map.Entry<Integer, Long> entry : entries)
                cache.invoke(entry.getKey(), INC);
        }
    }
}
