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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public class IgniteStreamerBenchmark extends IgniteAbstractBenchmark {
    /** */
    private List<String> cacheNames;

    /** */
    private ExecutorService executor;

    /** */
    private int entries;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        entries = args.range();

        if (entries <= 0)
            throw new IllegalArgumentException("Invalid number of entries: " + entries);

        if (cfg.threads() != 1)
            throw new IllegalArgumentException("IgniteStreamerBenchmark should be run with single thread. " +
                "Internally it starts multiple threads.");

        String cacheNamePrefix = args.streamerCachesPrefix();

        if (cacheNamePrefix == null || cacheNamePrefix.isEmpty())
            throw new IllegalArgumentException("Streamer caches prefix not set.");

        List<String> caches = new ArrayList<>();

        for (String cacheName : ignite().cacheNames()) {
            if (cacheName.startsWith(cacheNamePrefix))
                caches.add(cacheName);
        }

        if (caches.isEmpty())
            throw new IllegalArgumentException("Failed to find for IgniteStreamerBenchmark caches " +
                "starting with '" + cacheNamePrefix + "'");

        BenchmarkUtils.println("Found " + caches.size() + " caches for IgniteStreamerBenchmark: " + caches);

        if (args.streamerCacheIndex() >= caches.size()) {
            throw new IllegalArgumentException("Invalid streamer cache index: " + args.streamerCacheIndex() +
                ", there are only " + caches.size() + " caches.");
        }

        if (args.streamerCacheIndex() + args.streamerConcurrentCaches() > caches.size()) {
            throw new IllegalArgumentException("There are no enough caches [cacheIndex=" + args.streamerCacheIndex() +
                ", concurrentCaches=" + args.streamerConcurrentCaches() +
                ", totalCaches=" + caches.size() + ']');
        }

        Collections.sort(caches);

        cacheNames = new ArrayList<>(caches.subList(args.streamerCacheIndex(),
            args.streamerCacheIndex() + args.streamerConcurrentCaches()));

        executor = Executors.newFixedThreadPool(args.streamerConcurrentCaches());

        BenchmarkUtils.println("IgniteStreamerBenchmark start [cacheIndex=" + args.streamerCacheIndex() +
            ", concurrentCaches=" + args.streamerConcurrentCaches() +
            ", entries=" + entries +
            ", bufferSize=" + args.streamerBufferSize() +
            ", cachesToUse=" + cacheNames + ']');

        if (cfg.warmup() > 0) {
            BenchmarkUtils.println("IgniteStreamerBenchmark start warmup [warmupTimeMillis=" + cfg.warmup() + ']');

            final long warmupEnd = System.currentTimeMillis() + cfg.warmup();

            final AtomicBoolean stop = new AtomicBoolean();

            try {
                List<Future<Void>> futs = new ArrayList<>();

                for (final String cacheName : cacheNames) {
                    futs.add(executor.submit(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            Thread.currentThread().setName("streamer-" + cacheName);

                            BenchmarkUtils.println("IgniteStreamerBenchmark start warmup for cache " +
                                "[name=" + cacheName + ']');

                            final int KEYS = Math.min(100_000, entries);

                            int key = 1;

                            try (IgniteDataStreamer<Object, Object> streamer = ignite().dataStreamer(cacheName)) {
                                streamer.perNodeBufferSize(args.streamerBufferSize());

                                while (System.currentTimeMillis() < warmupEnd && !stop.get()) {
                                    for (int i = 0; i < 10; i++) {
                                        streamer.addData(-key++, new SampleValue(key));

                                        if (key >= KEYS)
                                            key = 1;
                                    }

                                    streamer.flush();
                                }
                            }

                            BenchmarkUtils.println("IgniteStreamerBenchmark finished warmup for cache " +
                                "[name=" + cacheName + ']');

                            return null;
                        }
                    }));
                }

                for (Future<Void> fut : futs)
                    fut.get();
            }
            finally {
                stop.set(true);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> map) throws Exception {
        BenchmarkUtils.println("IgniteStreamerBenchmark start test.");

        long start = System.currentTimeMillis();

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            List<Future<Void>> futs = new ArrayList<>();

            for (final String cacheName : cacheNames) {
                futs.add(executor.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        Thread.currentThread().setName("streamer-" + cacheName);

                        long start = System.currentTimeMillis();

                        BenchmarkUtils.println("IgniteStreamerBenchmark start load cache [name=" + cacheName + ']');

                        try (IgniteDataStreamer<Object, Object> streamer = ignite().dataStreamer(cacheName)) {
                            for (int i = 0; i < entries; i++) {
                                streamer.addData(i, new SampleValue(i));

                                if (i > 0 && i % 1000 == 0) {
                                    if (stop.get())
                                        break;

                                    if (i % 100_000 == 0) {
                                        BenchmarkUtils.println("IgniteStreamerBenchmark cache load progress [name=" + cacheName +
                                            ", entries=" + i +
                                            ", timeMillis=" + (System.currentTimeMillis() - start) + ']');
                                    }
                                }
                            }
                        }

                        long time = System.currentTimeMillis() - start;

                        BenchmarkUtils.println("IgniteStreamerBenchmark finished load cache [name=" + cacheName +
                            ", entries=" + entries +
                            ", bufferSize=" + args.streamerBufferSize() +
                            ", totalTimeMillis=" + time + ']');

                        return null;
                    }
                }));
            }

            for (Future<Void> fut : futs)
                fut.get();
        }
        finally {
            stop.set(true);
        }

        long time = System.currentTimeMillis() - start;

        BenchmarkUtils.println("IgniteStreamerBenchmark finished [totalTimeMillis=" + time +
            ", entries=" + entries +
            ", bufferSize=" + args.streamerBufferSize() + ']');

        for (String cacheName : cacheNames) {
            BenchmarkUtils.println("Cache size [cacheName=" + cacheName +
                ", size=" + ignite().cache(cacheName).size() + ']');
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        if (executor != null)
            executor.shutdown();

        super.tearDown();
    }
}
