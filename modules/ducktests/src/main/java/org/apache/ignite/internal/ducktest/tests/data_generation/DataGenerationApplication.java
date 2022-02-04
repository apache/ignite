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

package org.apache.ignite.internal.ducktest.tests.data_generation;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.ducktest.utils.metrics.MemoryUsageMetrics;

/**
 * Application generates cache data by specified parameters.
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Max streamer data size. */
    private static final int MAX_STREAMER_DATA_SIZE = 100_000_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = jsonNode.get("backups").asInt();
        int cacheCnt = jsonNode.get("cacheCount").asInt();
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();
        int threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        int timeoutSecs = Optional.ofNullable(jsonNode.get("timeoutSecs")).map(JsonNode::asInt).orElse(3600);

        markInitialized();

//        List<IgniteDataStreamer<Integer, BinaryObject>> streamers = new LinkedList<>();

//        CountDownLatch streamersLatch = new CountDownLatch(cacheCnt);

        ExecutorService executorSrvc = Executors.newFixedThreadPool(threads);

        for (int i = 1; i <= cacheCnt; i++) {
            String cacheName = "test-cache-" + i;
            log.info("Start with " + cacheName);
            IgniteCache<Integer, BinaryObject> cache = ignite.getOrCreateCache(
                new CacheConfiguration<Integer, BinaryObject>(cacheName)
                    .setBackups(backups));

            try (IgniteDataStreamer<Integer, BinaryObject> stmr = ignite.dataStreamer(cache.getName())) {
//            stmr.future().listen(new IgniteInClosure<IgniteFuture<?>>() {
//                @Override public void apply(IgniteFuture<?> fut) {
//                    streamersLatch.countDown();
//                }
//            });

//            streamers.add(stmr);

                CompletionService<Void> ecs = new ExecutorCompletionService<>(executorSrvc);

                int cnt = (to - from) / threads;
                int end = from;

                for (int j = 0; j < threads; j++) {
                    int start = end;
                    end += cnt;
                    if (end > to)
                        end = to;
                    ecs.submit(new GenerateCacheDataTask(stmr, entrySize, start, end), null);
                }

                for (int j = 0; j < threads; j++)
                    ecs.take().get();
            }
        }

        recordPeakMemory();
        executorSrvc.shutdown();
        try {
            if (executorSrvc.awaitTermination(timeoutSecs, TimeUnit.SECONDS)) {
//                for (IgniteDataStreamer<Integer, BinaryObject> streamer: streamers)
//                    streamer.close(false);
//                streamersLatch.await(timeoutSecs, TimeUnit.SECONDS);
//                recordPeakMemory();
                markFinished();
            } else {
//                recordPeakMemory();
                markBroken(new RuntimeException(String.format("timeout elapsed: %d secs", timeoutSecs)));
            }
        }
        catch (Throwable ex) {
//            recordPeakMemory();
            markBroken(new RuntimeException(String.format("error occured during execution: %s", ex.getMessage()), ex));
        }
    }

    /**
     *
     */
    private void recordPeakMemory() {
        recordResult("PEAK_HEAP_MEMORY", MemoryUsageMetrics.getPeakHeapMemory());
    }

    /**
     * Task to load data to cache via the provided streamer object.
     */
    private class GenerateCacheDataTask implements Runnable {
        /** Ignite Streamer. */
        private final IgniteDataStreamer<Integer, BinaryObject> stmr;
        /** Entry size. */
        private final int entrySize;
        /** Key value to start loading from. */
        private final int from;
        /** Key value (excluding) to stop loading. */
        private final int to;

        /**
         * @param stmr Ignite streamer object.
         * @param entrySize Entry size.
         * @param from From key.
         * @param to To key.
         */
        GenerateCacheDataTask(IgniteDataStreamer<Integer, BinaryObject> stmr, int entrySize, int from, int to) {
            this.stmr = stmr;
            this.entrySize = entrySize;
            this.from = from;
            this.to = to;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            generateCacheData();
        }

        /**
         * Generate cache data
         */
        private void generateCacheData() {
            int flushEach = MAX_STREAMER_DATA_SIZE / entrySize + (MAX_STREAMER_DATA_SIZE % entrySize == 0 ? 0 : 1);
            int logEach = (to - from) / 10;

            BinaryObjectBuilder builder = ignite.binary().builder("org.apache.ignite.ducktest.DataBinary");

            byte[] data = new byte[entrySize];

            ThreadLocalRandom.current().nextBytes(data);

            for (int i = from; i < to; i++) {
                builder.setField("key", i);
                builder.setField("data", data);

                stmr.addData(i, builder.build());

                if ((i - from + 1) % logEach == 0 && log.isDebugEnabled())
                    log.debug("Streamed " + (i - from + 1) + " entries into " + stmr.cacheName());

                if (i % flushEach == 0)
                    stmr.tryFlush();
            }

            log.info(stmr.cacheName() + " data generated [entryCnt=" + (to - from) + ", from=" + from +
                ", to=" + to + "]");
        }
    }
}
