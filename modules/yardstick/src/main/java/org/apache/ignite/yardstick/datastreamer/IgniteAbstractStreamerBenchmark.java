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

package org.apache.ignite.yardstick.datastreamer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.yardstick.IgniteBenchmarkArguments;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public abstract class IgniteAbstractStreamerBenchmark {
    /** */
    private String benchmarkName;

    /** */
    IgniteBenchmarkArguments args;

    /** */
    private List<String> cacheNames;

    /** */
    private ExecutorService executor;

    /** */
    private int entriesCnt;

    /** */
    public void setUp(
            String benchmarkName,
            IgniteBenchmarkArguments args,
            BenchmarkConfiguration cfg,
            Collection<String> allCacheNames
    ) throws Exception {
        this.benchmarkName = benchmarkName;
        this.args = args;

        entriesCnt = args.range();

        if (entriesCnt <= 0)
            throw new IllegalArgumentException("Invalid number of entries: " + entriesCnt);

        if (cfg.threads() != 1)
            throw new IllegalArgumentException(benchmarkName + " should be run with single thread. " +
                "Internally it starts multiple threads.");

        DataStreamerBenchmarkArguments streamerArgs = args.streamer;

        if (streamerArgs.batchSize() <= 0)
            throw new IllegalArgumentException("Invalid value for batch size property: " + streamerArgs.batchSize());

        String cacheNamePrefix = streamerArgs.cachesPrefix();

        if (cacheNamePrefix == null || cacheNamePrefix.isEmpty())
            throw new IllegalArgumentException("Streamer caches prefix not set.");

        List<String> caches = new ArrayList<>();

        for (String cacheName : allCacheNames) {
            if (cacheName.startsWith(cacheNamePrefix))
                caches.add(cacheName);
        }

        if (caches.isEmpty())
            throw new IllegalArgumentException("Failed to find for " + benchmarkName + " caches " +
                    "starting with '" + cacheNamePrefix + "'");

        BenchmarkUtils.println("Found " + caches.size() + " caches for " + benchmarkName + ": " + caches);

        if (streamerArgs.cacheIndex() >= caches.size()) {
            throw new IllegalArgumentException("Invalid streamer cache index: " + streamerArgs.cacheIndex() +
                    ", there are only " + caches.size() + " caches.");
        }

        if (streamerArgs.cacheIndex() + streamerArgs.concurrentCaches() > caches.size()) {
            throw new IllegalArgumentException("There are no enough caches [cacheIndex=" + streamerArgs.cacheIndex() +
                    ", concurrentCaches=" + streamerArgs.concurrentCaches() +
                    ", totalCaches=" + caches.size() + ']');
        }

        Collections.sort(caches);

        cacheNames = new ArrayList<>(caches.subList(streamerArgs.cacheIndex(),
                streamerArgs.cacheIndex() + streamerArgs.concurrentCaches()));

        if (streamerArgs.threadsPerCache() < 1)
            throw new IllegalArgumentException("Invalid number of threads per cache: " + streamerArgs.threadsPerCache());

        executor = Executors.newFixedThreadPool(streamerArgs.concurrentCaches() * streamerArgs.threadsPerCache());

        BenchmarkUtils.println(benchmarkName + " start [cacheIndex=" + streamerArgs.cacheIndex() +
            ", concurrentCaches=" + streamerArgs.concurrentCaches() +
            ", entries=" + entriesCnt +
            ", bufferSize=" + streamerArgs.bufferSize() +
            ", threadsPerCache=" + streamerArgs.threadsPerCache() +
            ", cachesToUse=" + cacheNames + ']');

        if (cfg.warmup() > 0) {
            BenchmarkUtils.println(benchmarkName + " start warmup [warmup=" + cfg.warmup() + ']');

            final long warmupEnd = System.currentTimeMillis() + cfg.warmup() * 1_000L;

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                List<Future<Void>> futs = new ArrayList<>();

                for (final String cacheName : cacheNames) {
                    AtomicInteger keyCnt = new AtomicInteger();

                    for (int t = 0; t < streamerArgs.threadsPerCache(); t++) {
                        final int threadNum = t;

                        futs.add(executor.submit(new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                Thread.currentThread().setName("streamer-" + cacheName + '-' + threadNum);

                                BenchmarkUtils.println(benchmarkName + " start warmup for cache [name=" + cacheName +
                                        ", threadNum=" + threadNum + ']');

                                final int KEYS = Math.min(100_000, entriesCnt);

                                try (DataStreamer<Object, Object> streamer = dataStreamer(cacheName)) {
                                    while (System.currentTimeMillis() < warmupEnd && !stop.get()) {
                                        int key = keyCnt.getAndIncrement() % KEYS + 1;

                                        for (int i = 0; i < 10; i++)
                                            streamer.addData(-key, new SampleValue(key));

                                        streamer.flush();
                                    }
                                }

                                BenchmarkUtils.println(benchmarkName + " finished warmup for cache " +
                                        "[name=" + cacheName + ", threadNum=" + threadNum + ']');

                                return null;
                            }
                        }));
                    }
                }

                for (Future<Void> fut : futs)
                    fut.get();
            }
            finally {
                stop.set(true);
            }
        }
    }

    /** */
    public void test() throws Exception {
        BenchmarkUtils.println(benchmarkName + " start test.");

        long start = System.currentTimeMillis();

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            List<Future<Void>> futs = new ArrayList<>();

            for (final String cacheName : cacheNames) {
                AtomicInteger keyCnt = new AtomicInteger();

                for (int t = 0; t < args.streamer.threadsPerCache(); t++) {
                    final int threadNum = t;

                    futs.add(executor.submit(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            Thread.currentThread().setName("streamer-" + cacheName + '-' + threadNum);

                            long start = System.currentTimeMillis();

                            BenchmarkUtils.println(benchmarkName + " start load cache [name=" + cacheName +
                                ", threadNum=" + threadNum + ']');

                            try (DataStreamer<Object, Object> streamer = dataStreamer(cacheName)) {
                                Map<Object, Object> entries = new HashMap<>(args.streamer.batchSize());

                                int key;
                                int i = 0;

                                while ((key = keyCnt.getAndIncrement()) < entriesCnt) {
                                    if (args.streamer.batchSize() == 1)
                                        streamer.addData(key, new SampleValue(key));
                                    else {
                                        entries.put(key, new SampleValue(key));

                                        if (entries.size() == args.streamer.batchSize()) {
                                            streamer.addData(entries);

                                            entries.clear();
                                        }
                                    }

                                    if (++i >= 1000) {
                                        i = 0;

                                        if (stop.get())
                                            break;
                                    }

                                    if (key % 100_000 == 0) {
                                        BenchmarkUtils.println(benchmarkName + " cache load progress [name=" + cacheName +
                                                ", entries=" + key +
                                                ", timeMillis=" + (System.currentTimeMillis() - start) + ']');
                                    }
                                }

                                if (!entries.isEmpty())
                                    streamer.addData(entries);
                            }

                            long time = System.currentTimeMillis() - start;

                            BenchmarkUtils.println(benchmarkName + " finished load cache [name=" + cacheName +
                                    ", entries=" + entriesCnt +
                                    ", threadNum=" + threadNum +
                                    ", bufferSize=" + args.streamer.bufferSize() +
                                    ", totalTimeMillis=" + time + ']');

                            return null;
                        }
                    }));
                }
            }

            for (Future<Void> fut : futs)
                fut.get();
        }
        finally {
            stop.set(true);
        }

        long time = System.currentTimeMillis() - start;

        BenchmarkUtils.println(benchmarkName + " finished [totalTimeMillis=" + time +
            ", entries=" + entriesCnt +
            ", bufferSize=" + args.streamer.bufferSize() + ']');

        for (String cacheName : cacheNames) {
            BenchmarkUtils.println("Cache size [cacheName=" + cacheName +
                ", size=" + cacheSize(cacheName) + ']');
        }
    }

    /** */
    public void tearDown() throws Exception {
        if (executor != null)
            executor.shutdown();
    }

    /** Gets data streamer facade. */
    abstract <K, V> DataStreamer<K, V> dataStreamer(String cacheName);

    /** */
    abstract int cacheSize(String cacheName);

    /** Common data streamer facade for thick and thin clients.  */
    interface DataStreamer<K, V> extends AutoCloseable {
        /** */
        public void addData(K key, V val);

        /** */
        public void addData(Map<K, V> entries);

        /** */
        public void flush();
    }
}
