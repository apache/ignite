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

package org.apache.ignite.internal.ducktest.tests.load;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Loading random uuids to cache.
 */
public class UuidDataLoaderApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws InterruptedException {
        String cacheName = jNode.get("cacheName").asText();

        long size = jNode.get("size").asLong();

        int dataSize = jNode.get("dataSize").asInt();

        markInitialized();

        long start = System.currentTimeMillis();

        int threads = Runtime.getRuntime().availableProcessors() / 2;

        long iterThread = size / threads;

        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++)
            new Thread(() -> {
                try (IgniteDataStreamer<UUID, byte[]> dataStreamer = ignite.dataStreamer(cacheName)) {
                    for (long j = 0L; j <= iterThread; j++)
                        dataStreamer.addData(UUID.randomUUID(), new byte[dataSize]);
                }

                latch.countDown();
            }).start();

        latch.await();

        recordResult("DURATION", System.currentTimeMillis() - start);

        markFinished();
    }
}
