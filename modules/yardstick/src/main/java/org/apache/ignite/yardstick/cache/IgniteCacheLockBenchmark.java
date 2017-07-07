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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs IgniteCache.lock operations.
 */
public class IgniteCacheLockBenchmark extends IgniteCacheAbstractBenchmark<String, Integer> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        final int n = nextRandom(args.range());
        final String key = "key" + n;
        final String otherKey = "other_" + key;
        final IgniteCache<String, Integer> cache = cacheForOperation();
        final Lock lock = cache.lock(key);

        lock.lock();

        try {
            cache.put(otherKey, cache.get(otherKey) + 1);
        }
        finally {
            lock.unlock();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Integer> cache() {
        return ignite().cache("tx");
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        loadCachesData();
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        loadValues(cacheName, args.preloadAmount());
    }

    /**
     * @param cacheName Cache name.
     * @param cnt Number of entries to load.
     */
    protected void loadValues(String cacheName, int cnt) {
        try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cacheName)) {
            for (int i = 0; i < cnt; i++) {
                dataLdr.addData("key" + i, new Integer(0));
                dataLdr.addData("other_key" + i, new Integer(0));

                if (i % 100000 == 0) {
                    if (Thread.currentThread().isInterrupted())
                        break;

                    println("Loaded entries [cache=" + cacheName + ", cnt=" + i + ']');
                }
            }
        }

        println("Load entries done [cache=" + cacheName + ", cnt=" + cnt + ']');
    }
}
