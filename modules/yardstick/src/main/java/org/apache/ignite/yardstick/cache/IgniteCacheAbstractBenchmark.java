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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Abstract class for Ignite benchmarks which use cache.
 */
public abstract class IgniteCacheAbstractBenchmark extends IgniteAbstractBenchmark {
    /** Cache. */
    protected IgniteCache<Integer, Object> cache;

    /** */
    private ThreadLocal<ThreadRange> threadRange = new ThreadLocal<>();

    /** */
    private AtomicInteger threadIdx = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();
    }

    /**
     * @return Range.
     */
    protected final ThreadRange threadRange() {
        ThreadRange r = threadRange.get();

        if (r == null) {
            if (args.keysPerThread()) {
                int idx = threadIdx.getAndIncrement();

                int keysPerThread = (int)(args.range() / (float)cfg.threads());

                int min = keysPerThread * idx;
                int max = min + keysPerThread;

                r = new ThreadRange(min, max);
            }
            else
                r = new ThreadRange(0, args.range());

            BenchmarkUtils.println(cfg, "Initialized thread range [min=" + r.min + ", max=" + r.max + ']');

            threadRange.set(r);
        }

        return r;
    }

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return IgniteCache Cache to use.
     */
    protected abstract IgniteCache<Integer, Object> cache();

    /**
     *
     */
    static class ThreadRange {
        /** */
        final int min;
        /** */
        final int max;

        /** */
        final ThreadLocalRandom rnd;

        /**
         * @param min Min.
         * @param max Max.
         */
        private ThreadRange(int min, int max) {
            this.min = min;
            this.max = max;

            rnd = ThreadLocalRandom.current();
        }

        /**
         * @return Next random key.
         */
        int nextRandom() {
            return rnd.nextInt(min, max);
        }
    }
}