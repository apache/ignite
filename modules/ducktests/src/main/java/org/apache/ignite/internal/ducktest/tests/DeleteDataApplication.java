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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import javax.cache.Cache;
import java.util.*;

/**
 * Loading random uuids in cache.
 */
public class DeleteDataApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) {
        String cacheName = jNode.get("cacheName").asText();

        int iterSize = Optional.ofNullable(jNode.get("iterSize"))
                .map(JsonNode::asInt)
                .orElse(50);

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(cacheName);

        long start = System.currentTimeMillis();

        int cnt = 0;

        markInitialized();

        Iterator<Cache.Entry<Object, Object>> iter = cache.iterator();

        Set<Object> keys = new HashSet<>();

        while (iter.hasNext() && cnt < iterSize) {
            keys.add(iter.next().getKey());

            cnt++;
        }

        cache.removeAll(keys);

        recordResult("DURATION", System.currentTimeMillis() - start);


        markFinished();
    }

//    /** */
//    private void workParallel(Ignite ignite, String cacheName, long iterSize, int dataSize) {
//        int core = Runtime.getRuntime().availableProcessors() / 2;
//
//        long iterCore = 0 < iterSize ? iterSize / core : iterSize;
//
//        ThreadFactory threadFactory = new ThreadFactoryBuilder()
//                .setNameFormat("UuidDataStreamer-%d")
//                .setDaemon(true)
//                .build();
//
//        ExecutorService executors = Executors.newFixedThreadPool(core, threadFactory);
//        CountDownLatch latch = new CountDownLatch(core);
//
//        for (int i = 0; i < core; i++)
//            executors.submit(new UuidDataStreamer(ignite, cacheName, latch, iterCore, dataSize));
//
//        try {
//            while (true) {
//                if (latch.await(1, TimeUnit.SECONDS) || terminated())
//                    break;
//            }
//        }
//        catch (InterruptedException e) {
//            markBroken(new RuntimeException("Unexpected thread interruption", e));
//
//            Thread.currentThread().interrupt();
//        }
//        finally {
//            executors.shutdownNow();
//        }
//    }
//
//    /** */
//    private class UuidDataStreamer implements Runnable {
//        /** Ignite. */
//        private final Ignite ignite;
//
//        /** Cache name. */
//        private final String cacheName;
//
//        /** Latch. */
//        private final CountDownLatch latch;
//
//        /** Iteration size. */
//        private final long iterSize;
//
//        /** Data size. */
//        private final int dataSize;
//
//        /** */
//        public UuidDataStreamer(Ignite ignite, String cacheName, CountDownLatch latch, long iterSize, int dataSize) {
//            this.ignite = ignite;
//            this.cacheName = cacheName;
//            this.latch = latch;
//            this.iterSize = iterSize;
//            this.dataSize = dataSize;
//        }
//
//        /** {@inheritDoc} */
//        @Override public void run() {
//            long cnt = 0L;
//
//            try (IgniteDataStreamer<UUID, byte[]> dataStreamer = ignite.dataStreamer(cacheName)) {
//                dataStreamer.autoFlushFrequency(100L);
//
//                while (cnt != iterSize && !Thread.currentThread().isInterrupted()) {
//                    UUID uuid = UUID.randomUUID();
//
//                    byte[] data = new byte[dataSize];
//
//                    ThreadLocalRandom.current().nextBytes(data);
//
//                    dataStreamer.addData(uuid, data);
//
//                    cnt++;
//                }
//
//                dataStreamer.flush();
//            }
//
//            latch.countDown();
//        }
//    }
}
