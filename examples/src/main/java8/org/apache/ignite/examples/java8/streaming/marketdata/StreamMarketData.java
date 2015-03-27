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

package org.apache.ignite.examples.java8.streaming.marketdata;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.stream.*;

import java.util.*;

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start streaming using {@link StreamMarketData}.</li>
 *     <li>Start querying top performing instruments using {@link QueryTopInstruments}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class StreamMarketData {
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

            // The cache is configured with sliding window holding 1 second of the streaming data.
            IgniteCache<String, Double> mktCache = ignite.getOrCreateCache(CacheConfig.marketTicksCache());
            IgniteCache<String, Instrument> instCache = ignite.getOrCreateCache(CacheConfig.instrumentCache());

            try (IgniteDataStreamer<String, Double> mktStmr = ignite.dataStreamer(mktCache.getName())) {
                // Note that we receive market data, but do not populate 'mktCache' (it remains empty).
                // Instead we update the instruments in the 'instCache'.
                mktStmr.receiver(StreamVisitor.from((cache, e) -> {
                    String symbol = e.getKey();
                    Double tick = e.getValue();

                    Instrument inst = instCache.get(symbol);

                    if (inst == null)
                        inst = new Instrument(symbol);

                    // Don't populate market cache, as we don't use it for querying.
                    // Update cached instrument based on the latest market tick.
                    inst.update(tick);

                    instCache.put(symbol, inst);
                }));

                // Stream market data into market data stream cache.
                while (true) {
                    for (int j = 0; j < INSTRUMENTS.length; j++) {
                        // Use gaussian distribution to ensure that
                        // numbers closer to 0 have higher probability.
                        double price = round2(INITIAL_PRICES[j] + RAND.nextGaussian());

                        mktStmr.addData(INSTRUMENTS[j], price);
                    }
                }
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
}
