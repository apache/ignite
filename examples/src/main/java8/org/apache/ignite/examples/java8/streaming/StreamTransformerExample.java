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

package org.apache.ignite.examples.java8.streaming;

import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.stream.StreamTransformer;

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start streaming using {@link StreamTransformerExample}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class StreamTransformerExample {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            CacheConfiguration<Integer, Long> cfg = new CacheConfiguration<>("randomNumbers");

            // Index key and value.
            cfg.setIndexedTypes(Integer.class, Long.class);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Long> stmCache = ignite.getOrCreateCache(cfg)) {
                try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                    // Allow data updates.
                    stmr.allowOverwrite(true);

                    // Configure data transformation to count random numbers added to the stream.
                    stmr.receiver(StreamTransformer.from((e, arg) -> {
                        // Get current count.
                        Long val = e.getValue();

                        // Increment count by 1.
                        e.setValue(val == null ? 1L : val + 1);

                        return null;
                    }));

                    // Stream 10 million of random numbers into the streamer cache.
                    for (int i = 1; i <= 10_000_000; i++) {
                        stmr.addData(RAND.nextInt(RANGE), 1L);

                        if (i % 500_000 == 0)
                            System.out.println("Number of tuples streamed into Ignite: " + i);
                    }
                }

                // Query top 10 most popular numbers every.
                SqlFieldsQuery top10Qry = new SqlFieldsQuery("select _key, _val from Long order by _val desc limit 10");

                // Execute queries.
                List<List<?>> top10 = stmCache.query(top10Qry).getAll();

                System.out.println("Top 10 most popular numbers:");

                // Print top 10 words.
                ExamplesUtils.printQueryResults(top10);
            }
        }
    }
}