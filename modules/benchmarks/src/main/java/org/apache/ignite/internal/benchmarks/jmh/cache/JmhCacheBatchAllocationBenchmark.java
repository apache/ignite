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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

/**
 * Cache operations on a batch of keys, for allocation runs with {@link JmhAllocationProfiler}. The setup writes the
 * keys and splits them into batches of consecutive keys, created up front with their values, in key order, as a
 * transactional {@code putAll} expects.
 */
@SuppressWarnings("unchecked")
public class JmhCacheBatchAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Keys in a batch. */
    @Param("10")
    private int batch;

    /** Batches: keys in order, with their values. */
    private Map<Integer, IntValue>[] batches;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        Integer[] keys = new Integer[CNT];
        IntValue[] vals = new IntValue[CNT];

        for (int i = 0; i < CNT; i++) {
            keys[i] = i;
            vals[i] = new IntValue(i);
        }

        populate(keys, vals);

        batches = new Map[CNT / batch];

        for (int b = 0; b < batches.length; b++) {
            batches[b] = new TreeMap<>();

            for (int i = b * batch; i < (b + 1) * batch; i++)
                batches[b].put(keys[i], vals[i]);

            // A map creates its key and value views on first use; Ignite and getAll() use them.
            batches[b].keySet();
            batches[b].values();
        }
    }

    /** Replaces the values of a batch with the same values. */
    @Benchmark
    public void putAll() {
        cache.putAll(batches[ThreadLocalRandom.current().nextInt(batches.length)]);
    }

    /** @return Values of a batch. */
    @Benchmark
    public Map<Integer, IntValue> getAll() {
        Set<Integer> keys = batches[ThreadLocalRandom.current().nextInt(batches.length)].keySet();

        return cache.getAll(keys);
    }
}
