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

package org.apache.ignite.internal.ducktest.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** */
    private static class Streamer implements Runnable {
        /** Cache name. */
        private final String cacheName;

        /** Ignite. */
        private final Ignite ignite;

        /** Working thread. */
        private final Thread thread;

        /** */
        private final AtomicLong counter;

        private final long durationNanos;

        private final int batchSize;

        private final int entrySize;

        /**
         * @param ignite Ignite.
         * @param cacheName Cache name.
         * @param threadName Thread name.
         * @param counter Shared counter.
         * @param duration Duration for data generation.
         * @param batchSize Bacth size.
         */
        public Streamer(Ignite ignite, String cacheName, String threadName, AtomicLong counter, long duration, int batchSize, int entrySize) {
            this.cacheName = cacheName;
            this.ignite = ignite;
            this.counter = counter;
            this.batchSize = batchSize;
            this.entrySize = entrySize;

            durationNanos = TimeUnit.SECONDS.toNanos(duration);
            thread = new Thread(this, threadName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            long endTime = System.nanoTime() + durationNanos;
            byte[] entry = new byte[entrySize];

            try (IgniteDataStreamer<Long, byte[]> stmr = ignite.dataStreamer(cacheName)) {
                do {
                    long start = counter.getAndAdd(batchSize);

                    for (long i = start; i < start + batchSize; i++)
                        stmr.addData(i, entry);
                }
                while (System.nanoTime() < endTime && !Thread.currentThread().isInterrupted());
            }
        }

        /**
         * Start thread.
         */
        public void start() {
            thread.start();
        }

        /**
         * Join working thread.
         * @throws InterruptedException If current thread was interrupted.
         */
        public void join() throws InterruptedException {
            thread.join();
        }
    }

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) {
        log.info("Creating cache...");

        String cacheName = jsonNode.get("cache_name").asText();
        int threadCnt = jsonNode.get("threads_count").asInt();
        int durationSec = jsonNode.get("duration").asInt();
        long startIdx = jsonNode.get("start") != null ? jsonNode.get("start").asLong() : 0;
        int entrySize = jsonNode.get("entry_size") != null ? jsonNode.get("entry_size").asInt() : 1024;

        ignite.getOrCreateCache(cacheName);

        List<Streamer> generators = new ArrayList<>(threadCnt - 1);

        AtomicLong sharedCntr = new AtomicLong(startIdx);

        for (int i = 0; i < threadCnt; i++) {
            Streamer streamer = new Streamer(ignite, cacheName, "stream-" + i, sharedCntr, durationSec, 1_000, entrySize);

            generators.add(streamer);

            if (i == threadCnt - 1) {
                streamer.run();

                break;
            }

            streamer.start();
        }

        for (int i = 0; i < generators.size(); i++) {
            try {
                generators.get(i).join();
            }
            catch (InterruptedException e) {
                log.error("Thread has been interrupted", e);

                Thread.currentThread().interrupt();
            }
        }

        recordResult("CACHE_SIZE", sharedCntr.get());

        markSyncExecutionComplete();
    }
}
