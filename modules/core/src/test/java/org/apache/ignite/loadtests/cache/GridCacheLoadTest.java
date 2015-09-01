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

package org.apache.ignite.loadtests.cache;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Cache load test.
 */
public final class GridCacheLoadTest extends GridCacheAbstractLoadTest {
    /** Memory test. */
    private static final boolean MEMORY = false;

    /** Load test. */
    private static final boolean LOAD = true;

    /** */
    private static final int KEY_RANGE = 1000;

    /** */
    private GridCacheLoadTest() {
        // No-op
    }

    /** Write closure. */
    private final CIX1<IgniteCache<Integer, Integer>> writeClos =
        new CIX1<IgniteCache<Integer, Integer>>() {
        @Override public void applyx(IgniteCache<Integer, Integer> cache) {
            for (int i = 0; i < operationsPerTx; i++) {
                int kv = RAND.nextInt(KEY_RANGE);

                cache.put(kv, kv);

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes");
            }
        }
    };

    /** Read closure. */
    private final CIX1<IgniteCache<Integer, Integer>> readClos =
        new CIX1<IgniteCache<Integer, Integer>>() {
        @Override public void applyx(IgniteCache<Integer, Integer> cache) {
            for (int i = 0; i < operationsPerTx; i++) {
                int k = RAND.nextInt(KEY_RANGE);

                Integer v = cache.get(k);

                if (v != null && !v.equals(k))
                    error("Invalid value [k=" + k + ", v=" + v + ']');

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads");
            }
        }
    };

    /**
     * @return New byte array.
     */
    private byte[] newArray() {
        byte[] bytes = new byte[valSize];

        // Populate one byte.
        bytes[RAND.nextInt(valSize)] = 1;

        return bytes;
    }

    /**
     *
     */
    @SuppressWarnings({"ErrorNotRethrown", "InfiniteLoopStatement"})
    private void memoryTest() {
        Ignite ignite = G.ignite();

        final IgniteCache<Integer, byte[]> cache = ignite.cache(null);

        assert cache != null;

        final AtomicInteger cnt = new AtomicInteger();

        try {
            GridTestUtils.runMultiThreaded(new Callable() {
                @Override public Object call() throws Exception {
                    while (true) {
                        int idx;

                        cache.put(idx = cnt.getAndIncrement(), newArray());

                        if (idx % 1000 == 0)
                            info("Stored '" + idx + "' objects in cache [cache-size=" + cache.size()
                                + ']');
                    }
                }
            }, threads, "memory-test-worker");
        }
        catch (OutOfMemoryError ignore) {
            info("Populated '" + cnt.get() + "' 1K objects into cache [cache-size=" + cache.size()
                + ']');
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args Command line.
     * @throws Exception If fails.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        System.out.println("Starting master node [params=" + Arrays.toString(args) + ']');

        String cfg = args.length >= 1 ? args[0] : CONFIG_FILE;
        String log = args.length >= 2 ? args[1] : LOG_FILE;

        final GridCacheLoadTest test = new GridCacheLoadTest();

        try (Ignite g = Ignition.start(test.configuration(cfg, log))) {
            System.gc();

            if (LOAD)
                test.loadTest(test.writeClos, test.readClos);

            G.ignite().cache(null).clear();

            System.gc();

            if (MEMORY)
                test.memoryTest();
        }
    }
}