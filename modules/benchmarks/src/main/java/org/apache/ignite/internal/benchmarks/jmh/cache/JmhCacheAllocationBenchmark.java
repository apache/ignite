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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.jmh.profile.JmhAllocationProfiler;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.TimeValue;

/** Single-key cache operations on keys and values created in setup, so measured allocations are Ignite's. */
@SuppressWarnings("unchecked")
public class JmhCacheAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Atomicity mode. */
    @Param("ATOMIC")
    private CacheAtomicityMode atomicity;

    /** SQL index on the value's field. */
    @Param("false")
    private boolean sqlIdx;

    /** Keys the setup doesn't write. */
    private Integer[] newKeys;

    /** Index of the next key in {@link #newKeys}. */
    private int newKeyIdx;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        populateIntValues();

        newKeys = new Integer[CNT];

        for (int i = 0; i < CNT; i++)
            newKeys[i] = CNT + i;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cfg = super.cacheConfiguration().setAtomicityMode(atomicity);

        if (sqlIdx) {
            cfg.setQueryEntities(List.of(new QueryEntity(Integer.class.getName(), IntValue.class.getName())
                .addQueryField("id", Integer.class.getName(), null)
                .setIndexes(List.of(new QueryIndex("id")))));
        }

        return cfg;
    }

    /** Replaces the value of a key with the same value: an update, not an insert. */
    @Benchmark
    public void put() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        cache.put(keys[i], vals[i]);
    }

    /** @return Value of a key. */
    @Benchmark
    public Object get() {
        return cache.get(keys[ThreadLocalRandom.current().nextInt(CNT)]);
    }

    /** @return Previous value of a key, replaced with the same value. */
    @Benchmark
    public Object getAndPut() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        return cache.getAndPut(keys[i], vals[i]);
    }

    /** @return {@code false}: the key exists, so the cache only checks it. */
    @Benchmark
    public boolean putIfAbsent() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        return cache.putIfAbsent(keys[i], vals[i]);
    }

    /** @return {@code true}: the key exists, and its value is replaced with the same value. */
    @Benchmark
    public boolean replace() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        return cache.replace(keys[i], vals[i]);
    }

    /** Inserts a new key and removes it, a new one each time: an atomic cache keeps a removed entry for a while. */
    @Benchmark
    public void putRemove() {
        int i = newKeyIdx++ % CNT;

        cache.put(newKeys[i], vals[i]);
        cache.remove(newKeys[i]);
    }

    /** Runs an entry processor that writes the value back; like a user's call, it allocates an empty args array. */
    @Benchmark
    public void invoke() {
        cache.invoke(keys[ThreadLocalRandom.current().nextInt(CNT)], SAME_VALUE);
    }

    /** Replaces the value of a key with the same value, asynchronously, and waits for it. */
    @Benchmark
    public void putAsync() {
        int i = ThreadLocalRandom.current().nextInt(CNT);

        cache.putAsync(keys[i], vals[i]).get();
    }

    /** @return Value of a key, read asynchronously. */
    @Benchmark
    public Object getAsync() {
        return cache.getAsync(keys[ThreadLocalRandom.current().nextInt(CNT)]).get();
    }

    /**
     * Runs put and get, atomic and transactional, with the allocation profiler; writes {@code <class name>.json}.
     *
     * @param args Arguments.
     */
    public static void main(String[] args) throws Exception {
        Options opts = JmhIdeBenchmarkRunner.create()
            .forks(1)
            .threads(1)
            .warmupIterations(5)
            .warmupTime(TimeValue.seconds(3))
            .measurementIterations(5)
            .measurementTime(TimeValue.seconds(3))
            .benchmarks(JmhCacheAllocationBenchmark.class.getSimpleName() + ".(put|get)$")
            .profilers(JmhAllocationProfiler.class)
            .jvmArguments("-Xms2g", "-Xmx2g", "-XX:-DoEscapeAnalysis")
            .optionsBuilder()
            .param("atomicity", "ATOMIC", "TRANSACTIONAL")
            .resultFormat(ResultFormatType.JSON)
            .result(JmhCacheAllocationBenchmark.class.getSimpleName() + ".json")
            .build();

        new Runner(opts).run();
    }
}
