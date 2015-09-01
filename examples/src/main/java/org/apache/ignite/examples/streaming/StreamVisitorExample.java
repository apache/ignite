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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ExamplesUtils;
import org.apache.ignite.stream.StreamVisitor;

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup}.</li>
 *     <li>Start streaming using {@link StreamVisitorExample}.</li>
 * </ul>
 */
public class StreamVisitorExample {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** The list of instruments. */
    private static final String[] INSTRUMENTS = {"IBM", "GOOG", "MSFT", "GE", "EBAY", "YHOO", "ORCL", "CSCO", "AMZN", "RHT"};

    /** The list of initial instrument prices. */
    private static final double[] INITIAL_PRICES = {194.9, 893.49, 34.21, 23.24, 57.93, 45.03, 44.41, 28.44, 378.49, 69.50};

    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            if (!ExamplesUtils.hasServerNodes(ignite))
                return;

            // Financial instrument cache configuration.
            CacheConfiguration<String, Instrument> instCfg = new CacheConfiguration<>("instCache");

            // Index key and value for querying financial instruments.
            // Note that Instrument class has @QuerySqlField annotation for secondary field indexing.
            instCfg.setIndexedTypes(String.class, Instrument.class);

            // Auto-close caches at the end of the example.
            try (
                IgniteCache<String, Double> mktCache = ignite.getOrCreateCache("marketTicks"); // Default config.
                IgniteCache<String, Instrument> instCache = ignite.getOrCreateCache(instCfg)
            ) {
                try (IgniteDataStreamer<String, Double> mktStmr = ignite.dataStreamer(mktCache.getName())) {
                    // Note that we receive market data, but do not populate 'mktCache' (it remains empty).
                    // Instead we update the instruments in the 'instCache'.
                    // Since both, 'instCache' and 'mktCache' use the same key, updates are collocated.
                    mktStmr.receiver(new StreamVisitor<String, Double>() {
                        @Override
                        public void apply(IgniteCache<String, Double> cache, Map.Entry<String, Double> e) {
                            String symbol = e.getKey();
                            Double tick = e.getValue();

                            Instrument inst = instCache.get(symbol);

                            if (inst == null)
                                inst = new Instrument(symbol);

                            // Don't populate market cache, as we don't use it for querying.
                            // Update cached instrument based on the latest market tick.
                            inst.update(tick);

                            instCache.put(symbol, inst);
                        }
                    });

                    // Stream 10 million market data ticks into the system.
                    for (int i = 1; i <= 10_000_000; i++) {
                        int idx = RAND.nextInt(INSTRUMENTS.length);

                        // Use gaussian distribution to ensure that
                        // numbers closer to 0 have higher probability.
                        double price = round2(INITIAL_PRICES[idx] + RAND.nextGaussian());

                        mktStmr.addData(INSTRUMENTS[idx], price);

                        if (i % 500_000 == 0)
                            System.out.println("Number of tuples streamed into Ignite: " + i);
                    }
                }

                // Select top 3 best performing instruments.
                SqlFieldsQuery top3qry = new SqlFieldsQuery(
                    "select symbol, (latest - open) from Instrument order by (latest - open) desc limit 3");

                // Execute queries.
                List<List<?>> top3 = instCache.query(top3qry).getAll();

                System.out.println("Top performing financial instruments: ");

                // Print top 10 words.
                ExamplesUtils.printQueryResults(top3);
            }
        }
    }

    /**
     * Rounds double value to two significant signs.
     *
     * @param val value to be rounded.
     * @return rounded double value.
     */
    private static double round2(double val) {
        return Math.floor(100 * val + 0.5) / 100;
    }

    /**
     * Financial instrument.
     */
    public static class Instrument implements Serializable {
        /** Instrument symbol. */
        @QuerySqlField(index = true)
        private final String symbol;

        /** Open price. */
        @QuerySqlField(index = true)
        private double open;

        /** Close price. */
        @QuerySqlField(index = true)
        private double latest;

        /**
         * @param symbol Symbol.
         */
        public Instrument(String symbol) {
            this.symbol = symbol;
        }

        /**
         * Updates this instrument based on the latest market tick price.
         *
         * @param price Latest price.
         */
        public void update(double price) {
            if (open == 0)
                open = price;

            this.latest = price;
        }
    }
}