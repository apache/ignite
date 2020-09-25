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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 *
 */
public class LoadGeneratorApplication extends IgniteAwareApplication {

    class Result {
        final long worst;
        final long ops;
        final double avgTime;

        public Result(long worst, long ops, long time) {
            this.worst = worst;
            this.ops = ops;

            avgTime = time / (double)ops;
        }
    }


    class LoadGenerator implements Runnable {
        private volatile Result res;

        private final IgniteCache<Long, String> cache;

        private final long range;

        private final int warmup;

        private final long durationNanos;

        public LoadGenerator(IgniteCache<Long, String> cache, long range, int warmup, int duration) {
            this.cache = cache;
            this.range = range;
            this.warmup = warmup;
            this.durationNanos = TimeUnit.SECONDS.toNanos(duration);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            int cnt = 0;

            long max = 0;

            boolean record = false;

            long endTime = Long.MAX_VALUE;
            long startTime = 0;

            while (System.nanoTime() < endTime && !terminated()) {
                cnt++;

                long start = System.nanoTime();

                cache.getAndPut(rnd.nextLong(range), UUID.randomUUID().toString());

                long finish = System.nanoTime();

                long time = finish - start;

                if (!record && cnt > warmup) {
                    record = true;

                    startTime = System.nanoTime();
                    endTime = System.nanoTime() + durationNanos;
                }

                if (record) {
                    if (max < time)
                        max = time;
                }

                if (cnt % 10_000 == 0)
                    log.info("Streamed " + cnt + " transactions [max=" + max + "]");

            }

            res = new Result(TimeUnit.NANOSECONDS.toMillis(max), cnt - warmup, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
        }

        Result result() {
            return res;
        }
    }
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) {
        IgniteCache<Long, String> cache = ignite.getOrCreateCache(jsonNode.get("cache_name").asText());

        int warmup = jsonNode.get("warmup").asInt();
        long range = jsonNode.get("range").asLong();
        int threadsCnt = jsonNode.get("threads").asInt();
        int durationSec = jsonNode.get("duration").asInt();

        List<LoadGenerator> gens = new ArrayList<>(threadsCnt);
        List<Thread> threads = new ArrayList<>(threadsCnt);

        for (int i = 0; i < threadsCnt; i++) {
            LoadGenerator dataGen = new LoadGenerator(cache, range, warmup, durationSec);

            Thread t = new Thread(dataGen, "load-generator-" + i);

            t.start();

            gens.add(dataGen);
            threads.add(t);
        }

        markInitialized();

        long maxLatency = -1;
        double opsDurationSum = 0;
        long totalOps = 0;

        for (int i = 0; i < threadsCnt; i++) {
            try {
                threads.get(i).join();
            }
            catch (InterruptedException e) {
                log.error("Thread was interrupted", e);
            }

            Result res = gens.get(i).result();

            if (maxLatency < res.worst)
                maxLatency = res.worst;

            opsDurationSum += res.avgTime;

            totalOps += res.ops;
        }

        recordResult("WORST_LATENCY", maxLatency);
        recordResult("STREAMED", totalOps);
        recordResult("AVG_OPERATION", new DecimalFormat("#0.00").format(opsDurationSum / threadsCnt));

        markFinished();
    }
}
