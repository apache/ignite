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

package org.apache.ignite.internal.trace.atomic.runners;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.trace.atomic.AtomicTraceUtils;
import org.jsr166.LongAdder8;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.trace.atomic.AtomicTraceUtils.CACHE_NAME;

/**
 * Atomic trace client runner.
 */
public class AtomicTraceClientRunner {
    /** Cache load threads count. */
    private static final int CACHE_LOAD_THREAD_CNT = 1;

    /** Cache size. */
    private static final int CACHE_SIZE = 1000;

    /** Adder. */
    private static final LongAdder8 ADDER = new LongAdder8();

    /**
     * Entry point.
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        // Start topology.
        Ignite node = Ignition.start(AtomicTraceUtils.config("cli", true));

        // Prepare cache.
        IgniteCache<Integer, Integer> cache = node.cache(CACHE_NAME);

        for (int i = 0; i < CACHE_SIZE; i++)
            cache.put(i, i);

        System.out.println(">>> Cache prepared.");

        // Start cache loaders.
        for (int i = 0; i < CACHE_LOAD_THREAD_CNT; i++) {
            Thread thread = new Thread(new CacheLoader(node));

            thread.start();
        }

        // Start throughput printer.
        Thread thread = new Thread(new ThroughputPrinter());

        thread.start();
    }

    /**
     * Cache load generator.
     */
    private static class CacheLoader implements Runnable {
        /** Index generator. */
        private static final AtomicInteger IDX_GEN = new AtomicInteger();

        /** Node. */
        private final Ignite node;

        /** Index. */
        private final int idx;

        /** Stop flag. */
        private volatile boolean stopped;

        /**
         * Constructor.
         *
         * @param node Node.
         */
        public CacheLoader(Ignite node) {
            this.node = node;

            idx = IDX_GEN.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println(">>> Cache loader " + idx + " started.");

            try {
                IgniteCache<Integer, Integer> cache = node.cache(CACHE_NAME);

                ThreadLocalRandom rand = ThreadLocalRandom.current();

                // Ensure threads are more or less distributed in time.
                try {
                    Thread.sleep(rand.nextInt(100, 2000));
                }
                catch (InterruptedException e) {
                    // No-op.
                }

                // Payload.
                while (!stopped) {
                    int key = rand.nextInt(CACHE_SIZE);

                    cache.put(key, key);

                    ADDER.add(1);
                }
            }
            finally {
                System.out.println(">>> Cache loader " + idx + " stopped.");
            }
        }

        /**
         * Stop thread.
         */
        public void stop() {
            stopped = true;
        }
    }

    /**
     * Throughput printer.
     */
    private static class ThroughputPrinter implements Runnable {
        /** {@inheritDoc} */
        @Override public void run() {
            System.out.println(">>> Throughput printer started.");

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            try {
                while (true) {
                    long beforeVal = ADDER.longValue();
                    long before = System.currentTimeMillis();

                    Thread.sleep(rnd.nextLong(3000, 5000));

                    long afterVal = ADDER.longValue();
                    long after = System.currentTimeMillis();

                    long deltaVal = afterVal - beforeVal;
                    long delta = after - before;

                    double t = 1000 * (double)deltaVal/(double)delta;

                    System.out.println(">>> " + t + " ops/sec");
                }
            }
            catch (Exception e) {
                System.out.println(">>> Throughput printer exception: " + e);
            }
            finally {
                System.out.println(">>> Throughput printer stopped.");
            }
        }
    }
}
