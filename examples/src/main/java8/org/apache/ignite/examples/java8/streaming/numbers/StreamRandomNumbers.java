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

package org.apache.ignite.examples.java8.streaming.numbers;

import org.apache.ignite.*;
import org.apache.ignite.examples.*;
import org.apache.ignite.stream.*;

import java.util.*;

/**
 * Stream random numbers into the streaming cache.
 * To start the example, you should:
 * <ul>
 *     <li>Start a few nodes using {@link ExampleNodeStartup} or by starting remote nodes as specified below.</li>
 *     <li>Start streaming using {@link StreamRandomNumbers}.</li>
 *     <li>Start querying popular numbers using {@link QueryPopularNumbers}.</li>
 * </ul>
 * <p>
 * You should start remote nodes by running {@link ExampleNodeStartup} in another JVM.
 */
public class StreamRandomNumbers {
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

            // The cache is configured with sliding window holding 1 second of the streaming data.
            IgniteCache<Integer, Long> stmCache = ignite.getOrCreateCache(CacheConfig.randomNumbersCache());

            try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                // Allow data updates.
                stmr.allowOverwrite(true);

                // Configure data transformation to count instances of the same word.
                stmr.receiver(StreamTransformer.from((e, arg) -> {
                    Long val = e.getValue();

                    e.setValue(val == null ? 1L : val + 1);

                    return null;
                }));

                // Stream random numbers into the streamer cache.
                while (true)
                    stmr.addData(RAND.nextInt(RANGE), 1L);
            }
        }
    }
}
