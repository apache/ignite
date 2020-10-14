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
package org.apache.ignite.snippets;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.stream.StreamReceiver;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.stream.StreamVisitor;
import org.junit.jupiter.api.Test;

public class DataStreaming {

    @Test
    void dataStreamerExample() {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Integer, String> cache = ignite.getOrCreateCache("myCache");
            //tag::dataStreamer1[]
            // Get the data streamer reference and stream data.
            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer("myCache")) {
                // Stream entries.
                for (int i = 0; i < 100000; i++)
                    stmr.addData(i, Integer.toString(i));
                //end::dataStreamer1[]
                //tag::dataStreamer2[]
                stmr.allowOverwrite(true);
                //end::dataStreamer2[]
                //tag::dataStreamer1[]
            }
            System.out.println("dataStreamerExample output:" + cache.get(99999));
            //end::dataStreamer1[]
        }
    }

    @Test
    void streamTransformerExample() {
        try (Ignite ignite = Ignition.start()) {
            //tag::streamTransformer[]
            String[] text = { "hello", "world", "hello", "Ignite" };
            CacheConfiguration<String, Long> cfg = new CacheConfiguration<>("wordCountCache");

            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache(cfg);

            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                // Allow data updates.
                stmr.allowOverwrite(true);

                // Configure data transformation to count instances of the same word.
                stmr.receiver(StreamTransformer.from((e, arg) -> {
                    // Get current count.
                    Long val = e.getValue();

                    // Increment count by 1.
                    e.setValue(val == null ? 1L : val + 1);

                    return null;
                }));

                // Stream words into the streamer cache.
                for (String word : text)
                    stmr.addData(word, 1L);

            }
            //end::streamTransformer[]
            System.out.println("StreamTransformer example output:" + stmCache.get("hello"));
        }
    }

    @Test
    void streamReceiverExample() {
        try (Ignite ignite = Ignition.start()) {
            ignite.getOrCreateCache("myCache");
            //tag::streamReceiver[]
            try (IgniteDataStreamer<Integer, String> stmr = ignite.dataStreamer("myCache")) {

                stmr.allowOverwrite(true);

                stmr.receiver((StreamReceiver<Integer, String>) (cache, entries) -> entries.forEach(entry -> {

                    // do something with the entry

                    cache.put(entry.getKey(), entry.getValue());
                }));
            }
            //end::streamReceiver[]
        }
    }

    @Test
    void poolSize() {
        //tag::pool-size[] 
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setDataStreamerThreadPoolSize(10);

        Ignite ignite = Ignition.start(cfg);
        //end::pool-size[] 
        ignite.close();
    }

    // tag::stream-visitor[]
    static class Instrument {
        final String symbol;
        Double latest;
        Double high;
        Double low;

        public Instrument(String symbol) {
            this.symbol = symbol;
        }

    }

    static Map<String, Double> getMarketData() {
        //populate market data somehow
        return new HashMap<>();
    }

    @Test
    void streamVisitorExample() {
        try (Ignite ignite = Ignition.start()) {
            CacheConfiguration<String, Double> mrktDataCfg = new CacheConfiguration<>("marketData");
            CacheConfiguration<String, Instrument> instCfg = new CacheConfiguration<>("instruments");

            // Cache for market data ticks streamed into the system.
            IgniteCache<String, Double> mrktData = ignite.getOrCreateCache(mrktDataCfg);

            // Cache for financial instruments.
            IgniteCache<String, Instrument> instCache = ignite.getOrCreateCache(instCfg);

            try (IgniteDataStreamer<String, Double> mktStmr = ignite.dataStreamer("marketData")) {
                // Note that we do not populate the 'marketData' cache (it remains empty).
                // Instead we update the 'instruments' cache based on the latest market price.
                mktStmr.receiver(StreamVisitor.from((cache, e) -> {
                    String symbol = e.getKey();
                    Double tick = e.getValue();

                    Instrument inst = instCache.get(symbol);

                    if (inst == null)
                        inst = new Instrument(symbol);

                    // Update instrument price based on the latest market tick.
                    inst.high = Math.max(inst.high, tick);
                    inst.low = Math.min(inst.low, tick);
                    inst.latest = tick;

                    // Update the instrument cache.
                    instCache.put(symbol, inst);
                }));

                // Stream market data into the cluster.
                Map<String, Double> marketData = getMarketData();
                for (Map.Entry<String, Double> tick : marketData.entrySet())
                    mktStmr.addData(tick);
            }
        }
    }
    // end::stream-visitor[]
}
