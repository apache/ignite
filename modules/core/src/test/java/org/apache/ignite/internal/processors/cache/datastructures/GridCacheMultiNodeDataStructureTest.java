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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.internal.util.typedef.*;

/**
 * Starts up an node with cache configuration.
 * You can also must start a stand-alone GridGain instance by passing the path
 * to configuration file to {@code 'ggstart.{sh|bat}'} script, like so:
 * {@code 'ggstart.sh examples/config/example-cache.xml'}.
 */
public class GridCacheMultiNodeDataStructureTest {
    /** Ensure singleton. */
    private GridCacheMultiNodeDataStructureTest() { /* No-op. */ }

    /**
     * Put data to cache and then queries them.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteCheckedException If example execution failed.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        try (Ignite g = G.start("examples/config/example-cache.xml")) {
            // All available nodes.
            if (g.cluster().nodes().size() <= 2)
                throw new IgniteCheckedException("At least 2 nodes must be started.");

            sample(g, "partitioned");
            sample(g, "replicated");
            sample(g, "local");
        }
    }

    /**
     *
     * @param g Grid.
     * @param cacheName Cache name.
     * @throws IgniteCheckedException If failed.
     */
    private static void sample(Ignite g, String cacheName) throws IgniteCheckedException {
        GridCache<Long, Object> cache = g.cache(cacheName);

        CacheAtomicLong atomicLong = cache.dataStructures().atomicLong("keygen", 0, true);

        CacheAtomicSequence seq = cache.dataStructures().atomicSequence("keygen", 0, true);

        seq.incrementAndGet();
        seq.incrementAndGet();

        seq.incrementAndGet();
        seq.incrementAndGet();

        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();
        atomicLong.incrementAndGet();

        X.println(cacheName+": Seq: " + seq.get() + " atomicLong " + atomicLong.get());
    }
}
