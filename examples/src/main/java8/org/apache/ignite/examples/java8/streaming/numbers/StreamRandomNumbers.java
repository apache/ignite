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
import org.apache.ignite.examples.java8.*;
import org.apache.ignite.stream.*;

import java.util.*;

/**
 * Real time popular numbers counter.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-compute.xml'}.
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will
 * start node with {@code examples/config/example-compute.xml} configuration.
 */
public class StreamRandomNumbers {
    /** Random number generator. */
    private static final Random RAND = new Random();

    /** Range within which to generate numbers. */
    private static final int RANGE = 1000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Mark this cluster member as client.
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start("examples/config/example-compute.xml")) {
            // Create new cache or get existing one.
            try (IgniteCache<Integer, Long> stmCache = ignite.createCache(CacheConfig.configure())) {
                if (!ExamplesUtils.hasServerNodes(ignite))
                    return;

                try (IgniteDataStreamer<Integer, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                    // Allow data updates.
                    stmr.allowOverwrite(true);

                    // Configure data transformation to count instances of the same word.
                    stmr.receiver(new StreamTransformer<>((e, arg) -> {
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
}
