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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Application generates cache data by specified parameters.
 */
public class DataGenerationApplication extends IgniteAwareApplication {
    /** Max streamer data size. */
    private static final int MAX_STREAMER_DATA_SIZE = 500_000_000;

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        int backups = jsonNode.get("backups").asInt();
        Integer cacheCnt = Optional.ofNullable(jsonNode.get("cacheCount")).map(JsonNode::asInt).orElse(null);
        String cacheNameTemplate = Optional.ofNullable(jsonNode.get("cacheNameTemplate"))
                .filter(n->!n.isNull())
                .map(JsonNode::asText)
                .orElse("test-cache-%d");
        List<String> cacheNames = Optional.ofNullable(jsonNode.get("cacheNames"))
                .filter(JsonNode::isArray)
                .map(JsonNode::elements)
                .map(names -> {
                    List<String> tmp = new ArrayList<>();
                    names.forEachRemaining(element -> tmp.add(element.asText()));
                    return tmp;
                })
                .orElse(null);
        int entrySize = jsonNode.get("entrySize").asInt();
        int from = jsonNode.get("from").asInt();
        int to = jsonNode.get("to").asInt();
        int preloaders = Optional.ofNullable(jsonNode.get("preloaders")).map(JsonNode::asInt).orElse(1);
        String preloadersToken = Optional.ofNullable(jsonNode.get("preloadersToken")).map(JsonNode::asText).orElse(null);
        int threads = Optional.ofNullable(jsonNode.get("threads")).map(JsonNode::asInt).orElse(1);
        int timeoutSecs = Optional.ofNullable(jsonNode.get("timeoutSecs")).map(JsonNode::asInt).orElse(3600);

        if (preloaders < 0)
            throw new RuntimeException("preloaders parameter should be > 0");

        if (preloaders > 1 && preloadersToken == null)
            throw new RuntimeException("preloadersToken should be passed if preloaders > 1");

        if (cacheCnt == null && cacheNames == null)
            throw new RuntimeException("either cacheCount or cacheNames should be passed");;

        markInitialized();

        if (cacheNames == null) {
            cacheNames = IntStream.rangeClosed(1, cacheCnt).boxed()
                    .map(i -> String.format(cacheNameTemplate, i))
                    .collect(Collectors.toList());
        }

        IgniteCountDownLatch preloadersLatch = null;
        if (preloaders > 1) {
            preloadersLatch = ignite.countDownLatch("DataGenerationApplication-latch-" + preloadersToken,
                    preloaders, true, true);
        }

        ExecutorService executorSrvc = Executors.newFixedThreadPool(threads);
        for (String cacheName : cacheNames) {
            log.info("Create cache: " + cacheName);
            ignite.getOrCreateCache(
                    new CacheConfiguration<Integer, BinaryObject>(cacheName)
                            .setBackups(backups));

            if (persistenceEnabled(ignite, cacheName)) {
                log.info("Disable WAL for cache: " + cacheName);
                ignite.cluster().disableWal(cacheName);
            }
        }

        for (String cacheName : cacheNames) {
            log.info("Start with " + cacheName);

            try (IgniteDataStreamer<Integer, BinaryObject> stmr = ignite.dataStreamer(cacheName)) {
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

        if (preloadersLatch != null) {
            preloadersLatch.countDown();
            preloadersLatch.await(timeoutSecs, TimeUnit.SECONDS);
        }

        for (String cacheName : cacheNames) {
            if (persistenceEnabled(ignite, cacheName)) {
                log.info("Enable WAL for cache: " + cacheName);
                ignite.cluster().enableWal(cacheName);
            }
        }

        recordPeakMemory();
        executorSrvc.shutdown();
        try {
            if (executorSrvc.awaitTermination(timeoutSecs, TimeUnit.SECONDS)) {
                markFinished();
            } else {
                markBroken(new RuntimeException(String.format("timeout elapsed: %d secs", timeoutSecs)));
            }
        }
        catch (Throwable ex) {
            markBroken(new RuntimeException(String.format("error occurred during execution: %s", ex.getMessage()), ex));
        }
    }

    private boolean persistenceEnabled(Ignite ignite, String cacheName) {
        DataRegionConfiguration[] dataRegionConfigurations = ignite.configuration().getDataStorageConfiguration()
                .getDataRegionConfigurations();

        if (dataRegionConfigurations == null) {
            return ignite.configuration().getDataStorageConfiguration()
                    .getDefaultDataRegionConfiguration().isPersistenceEnabled();
        } else {
            String dataRegionName = ignite.cache(cacheName).getConfiguration(CacheConfiguration.class).getDataRegionName();
            return Arrays.stream(ignite.configuration().getDataStorageConfiguration().getDataRegionConfigurations())
                    .filter(dr -> dr.getName().equals(dataRegionName))
                    .findFirst()
                    .map(DataRegionConfiguration::isPersistenceEnabled)
                    .orElseThrow(() -> new RuntimeException(String
                            .format("FATAL: Data region `%s` wasn't found for cache `%s`", dataRegionName, cacheName)));
        }
    }

    /**
     *
     */
    private void recordPeakMemory() {
        long totalPeakHeapBytes = 0;
        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (MemoryPoolMXBean pool : memoryPoolMXBeans) {
            if (pool.getType() == MemoryType.HEAP)
                totalPeakHeapBytes += pool.getPeakUsage().getUsed();
        }
        recordResult("PEAK_HEAP_MEMORY", totalPeakHeapBytes);
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
         * Generate cache  data
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
