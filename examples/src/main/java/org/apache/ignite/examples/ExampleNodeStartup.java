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

package org.apache.ignite.examples;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Starts up an empty node with example compute configuration.
 */
public class ExampleNodeStartup {
    /**
     * Start up an empty node with example compute configuration.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignite ig = Ignition.start("examples/config/example-ignite.xml");

        ig.cluster().setBaselineTopology(ig.cluster().nodes());

        if (ig.cache("cache1") == null) {
            ig.createCache(new CacheConfiguration<>().setName("cache1").setCacheMode(CacheMode.REPLICATED).setStatisticsEnabled(true));
            ig.createCache(new CacheConfiguration<>().setName("cache2").setCacheMode(CacheMode.REPLICATED).setStatisticsEnabled(true));
            ig.createCache(new CacheConfiguration<>().setName("cache3").setCacheMode(CacheMode.REPLICATED).setStatisticsEnabled(true));

            try (IgniteDataStreamer<Integer, Integer> streamer = ig.dataStreamer("cache1")) {
                for (int k = 0; k < 1_000_000; k++)
                    streamer.addData(k, k);
            }

            try (IgniteDataStreamer<Integer, Integer> streamer = ig.dataStreamer("cache2")) {
                for (int k = 0; k < 1_000_000; k++)
                    streamer.addData(k, k);
            }

            try (IgniteDataStreamer<Integer, Integer> streamer = ig.dataStreamer("cache3")) {
                for (int k = 0; k < 1_000_000; k++)
                    streamer.addData(k, k);
            }
        }
    }
}
