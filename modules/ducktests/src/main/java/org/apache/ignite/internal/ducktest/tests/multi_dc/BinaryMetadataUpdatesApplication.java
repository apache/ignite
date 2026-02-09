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

package org.apache.ignite.internal.ducktest.tests.multi_dc;

import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.BasicRateLimiter;

/**
 * Application continuously writes to cache with fixed rps to update binary metadata and
 * trigger discovery messages within cluster.
 */
public class BinaryMetadataUpdatesApplication extends IgniteAwareApplication {
    /** */
    private static final int RANDOM_INTEGER_BOUND = 1_000;

    /** */
    private static final int RANDOM_BYTE_ARRAY_SIZE = 3;

    /** */
    private static final String ENTRY_VERSION_FIELD_NAME = "version";

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /** */
    private final Queue<Object> binObjHolder = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected void run(JsonNode jsonNode) throws Exception {
        String cacheName = jsonNode.get("cacheName").asText();

        int threadCnt = Optional.ofNullable(jsonNode.get("threadCnt"))
            .map(JsonNode::asInt)
            .orElse(1);

        int rps = Optional.ofNullable(jsonNode.get("rps"))
            .map(JsonNode::asInt)
            .orElse(100);

        int objCnt = Optional.ofNullable(jsonNode.get("objectCnt"))
            .map(JsonNode::asInt)
            .orElse(100);

        log.info("Test props: cacheName={}, threadCnt={}, rps={}", cacheName, threadCnt, rps);

        markInitialized();

        prepareData(threadCnt, objCnt);
        log.info("Prepare BinaryObjects is completed");

        int putCnt = loadUntilTerminated(threadCnt, rps, objCnt, cacheName);

        checkData(cacheName, putCnt);
        log.info("Data check is successful");

        recordResult("putCnt", putCnt);

        markFinished();
    }

    /** */
    private void prepareData(int threadCnt, int objCnt) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(threadCnt);
        CountDownLatch latch = new CountDownLatch(objCnt);

        for (int i = 0; i < objCnt; i++) {
            final int idx = i;

            pool.submit(() -> {
                Object obj = getBinaryObject(idx);

                binObjHolder.add(obj);
                latch.countDown();
            });
        }

        latch.await();
        pool.shutdown();
    }

    /** */
    private int loadUntilTerminated(int threadCnt, int rps, int objCnt, String cacheName) throws InterruptedException {
        IgniteCache<Object, Object> cache = ignite.cache(cacheName).withKeepBinary();

        ExecutorService pool = Executors.newFixedThreadPool(threadCnt);
        BasicRateLimiter limiter = new BasicRateLimiter(rps);

        AtomicInteger idx = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(threadCnt);

        long startNs = System.nanoTime();

        for (int t = 0; t < threadCnt; t++) {
            pool.submit(() -> {
                while (!terminated()) {
                    int key = idx.get();

                    if (key >= objCnt)
                        break;

                    if (idx.compareAndSet(key, key + 1)) {
                        Object val = binObjHolder.poll();

                        try {
                            limiter.acquire(1);
                        }
                        catch (IgniteInterruptedCheckedException e) {
                            throw new RuntimeException(e);
                        }

                        cache.put(key, val);
                    }
                }

                latch.countDown();
            });
        }

        latch.await();

        long endNs = System.nanoTime();

        pool.shutdown();

        double durationSec = (endNs - startNs) / 1_000_000_000.0;
        double actualRps = idx.get() / durationSec;

        log.info("Data load is finished [putCnt={}, rps={}]", idx.get(), actualRps);

        while (!terminated())
            Thread.sleep(100);

        return idx.get();
    }

    /** */
    private void checkData(String cacheName, int putCnt) {
        IgniteCache<Object, Object> cache = ignite.cache(cacheName).withKeepBinary();

        assert cache.size() == putCnt : "Unexpected cache size [putCnt=" + putCnt + ", size=" + cache.size() + "]";

        for (int i = 0; i < putCnt; i++)
            assert cache.get(i) != null : "Expected value is absent [key=" + i + "]";
    }

    /** */
    private BinaryObject getBinaryObject(int idx) {
        Object val = getRandomObject();

        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

        int entryVer = idx + 1;
        String entryNewFieldName = "field" + entryVer;

        builder.setField(ENTRY_VERSION_FIELD_NAME, entryVer);
        builder.setField(entryNewFieldName, val);

        return builder.build();
    }

    /** */
    private static Object getRandomObject() {
        int fieldTypeIdx = ThreadLocalRandom.current().nextInt(4);

        switch (fieldTypeIdx % 4) {
            case 0:
                return getRandomInteger();
            case 1:
                return getRandomString();
            case 2:
                return getRandomByteArray();
            case 3:
                return new Object();
        }

        throw new RuntimeException("Error generating random object");
    }

    /** */
    private static String getRandomString() {
        return "str-" + getRandomInteger();
    }

    /** */
    private static int getRandomInteger() {
        return ThreadLocalRandom.current().nextInt(RANDOM_INTEGER_BOUND);
    }

    /** */
    private static byte[] getRandomByteArray() {
        byte[] res = new byte[RANDOM_BYTE_ARRAY_SIZE];
        ThreadLocalRandom.current().nextBytes(res);
        return res;
    }
}
