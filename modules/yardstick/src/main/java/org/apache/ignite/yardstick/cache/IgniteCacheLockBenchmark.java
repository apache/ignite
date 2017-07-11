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
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs IgniteCache.lock operations.
 */
public class IgniteCacheLockBenchmark extends IgniteCacheAbstractBenchmark<String, Integer> {
    /** Cache lock. */
    private Lock lock;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        lock.lock();
        lock.unlock();

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<String, Integer> cache() {
        return ignite().cache("tx");
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        String key = "key";

        IgniteCache<String, Integer> cache = cacheForOperation();

        cache.put(key, 0);

        lock = cache.lock(key);
    }
}
