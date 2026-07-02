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

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Full client-to-server cycle benchmark: every operation goes from a client node through communication
 * to 2 data nodes and back, exercising the message marshalling stack end-to-end.
 *
 * <p>Operations cover distinct message families: {@code put} (single atomic update / tx prepare+finish),
 * {@code putAll} (batch update), {@code get} (near get request/response with a {@code CacheObject} payload),
 * {@code invoke} (entry processor marshalled into the update request). Value variants: multi-field POJO
 * (binary object) and a raw 4KB byte array.
 */
@SuppressWarnings("unchecked")
public class JmhCacheFullCycleBenchmark extends JmhCacheAbstractBenchmark {
    /** Keys count; smaller than the default {@link #CNT} since values are heavy. */
    private static final int KEYS = 10_000;

    /** Batch size for {@link #putAll()}. */
    private static final int BATCH = 10;

    /** Value variant. */
    @Param({"POJO", "BYTES_4K"})
    private String valType;

    /** Reused 4KB payload. */
    private byte[] payload;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        payload = new byte[4096];

        ThreadLocalRandom.current().nextBytes(payload);

        try (IgniteDataStreamer<Integer, Object> ldr = node.dataStreamer(cache.getName())) {
            for (int i = 0; i < KEYS; i++)
                ldr.addData(i, value(i));
        }

        System.out.println("Cache populated [valType=" + valType + ']');
    }

    /** @return Value of the current variant for {@code key}. */
    private Object value(int key) {
        return "POJO".equals(valType) ? new Person(key, "name-" + key, key * 1000L) : payload;
    }

    /** Single update: atomic single-update request or implicit-tx prepare/finish chain. */
    @Benchmark
    public void put() {
        int key = ThreadLocalRandom.current().nextInt(KEYS);

        cache.put(key, value(key));
    }

    /** Single read: near get request/response carrying a {@code CacheObject}. */
    @Benchmark
    public Object get() {
        return cache.get(ThreadLocalRandom.current().nextInt(KEYS));
    }

    /** Batch update: full-update request / multi-entry tx. Sorted keys keep the tx deadlock-free. */
    @Benchmark
    public void putAll() {
        int base = ThreadLocalRandom.current().nextInt(KEYS - BATCH);

        Map<Integer, Object> map = new TreeMap<>();

        for (int i = 0; i < BATCH; i++)
            map.put(base + i, value(base + i));

        cache.putAll(map);
    }

    /** Entry processor round-trip: the processor is marshalled into the update request. */
    @Benchmark
    public Object invoke() {
        return cache.invoke(ThreadLocalRandom.current().nextInt(KEYS), new TouchProcessor());
    }

    /**
     * Run benchmarks for both atomicity modes. Runs in-process ({@code forks(0)}), so pass {@code --add-opens} JVM
     * options and the heap size to the launching JVM.
     *
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        CacheAtomicityMode[] modes = args.length > 0
            ? new CacheAtomicityMode[] {CacheAtomicityMode.valueOf(args[0])}
            : new CacheAtomicityMode[] {CacheAtomicityMode.ATOMIC, CacheAtomicityMode.TRANSACTIONAL};

        for (CacheAtomicityMode mode : modes) {
            System.setProperty(PROP_ATOMICITY_MODE, mode.name());
            System.setProperty(PROP_WRITE_SYNC_MODE, CacheWriteSynchronizationMode.FULL_SYNC.name());
            System.setProperty(PROP_DATA_NODES, "2");
            System.setProperty(PROP_CLIENT_MODE, "true");

            JmhIdeBenchmarkRunner.create()
                .forks(0)
                .threads(1)
                .benchmarkModes(Mode.AverageTime)
                .outputTimeUnit(TimeUnit.MICROSECONDS)
                .warmupIterations(5)
                .warmupTime(TimeValue.seconds(2))
                .measurementIterations(8)
                .measurementTime(TimeValue.seconds(2))
                .benchmarks(JmhCacheFullCycleBenchmark.class.getSimpleName())
                .profilers(GCProfiler.class)
                .run();
        }
    }

    /** Multi-field value marshalled as a binary object. */
    public static class Person implements Serializable {
        /** */
        private final int id;

        /** */
        private final String name;

        /** */
        private final long salary;

        /**
         * @param id Id.
         * @param name Name.
         * @param salary Salary.
         */
        public Person(int id, String name, long salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }
    }

    /** Read-only processor: forces the entry-processor marshalling round-trip. */
    public static class TouchProcessor implements CacheEntryProcessor<Integer, Object, Object> {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> e, Object... args) {
            return e.exists();
        }
    }
}
