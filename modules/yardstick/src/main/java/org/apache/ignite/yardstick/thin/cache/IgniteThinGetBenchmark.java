/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.thin.cache;

import java.util.Map;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Thin client benchmark that performs get operations.
 */
public class IgniteThinGetBenchmark extends IgniteThinCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (args.preloadAmount() > args.range())
            throw new IllegalArgumentException("Preloading amount (\"-pa\", \"--preloadAmount\") " +
                "must by less then the range (\"-r\", \"--range\").");

        loadSampleValues(cache().getName(), args.preloadAmount());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        cache().get(key);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected ClientCache<Integer, Object> cache() {
        return client().cache("atomic");
    }

    /**
     * @param cacheName Cache name.
     * @param cnt Number of entries to load.
     */
    private void loadSampleValues(String cacheName, int cnt) {
        for (int i = 0; i < cnt; i++) {
            cache.put(i, new SampleValue(i));

            if (i % 100000 == 0) {
                if (Thread.currentThread().isInterrupted())
                    break;

                println("Loaded entries [cache=" + cacheName + ", cnt=" + i + ']');
            }
        }

        println("Load entries done [cache=" + cacheName + ", cnt=" + cnt + ']');
    }
}
