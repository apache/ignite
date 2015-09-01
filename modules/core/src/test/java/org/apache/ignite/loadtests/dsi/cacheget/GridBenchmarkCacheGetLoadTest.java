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

package org.apache.ignite.loadtests.dsi.cacheget;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;

/**
 * Benchmark cache get load test.
 */
public class GridBenchmarkCacheGetLoadTest {
    /** */
    private static AtomicLong cnt = new AtomicLong();

    /** */
    private static AtomicLong latency = new AtomicLong();

    /** */
    private static AtomicLong id = new AtomicLong();

    private static Thread t;

    /**
     *
     */
    private GridBenchmarkCacheGetLoadTest() {
        // No-op.
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        Ignition.start("modules/core/src/test/config/load/dsi-49-server-production.xml");

        IgniteCache<Long, Long> cache = Ignition.ignite("dsi").cache("PARTITIONED_CACHE");

        stats();

        for (int i = 0; i < 5000000; i++) {
            long t0 = System.currentTimeMillis();

            cnt.incrementAndGet();

            cache.get(id.incrementAndGet());

            latency.addAndGet(System.currentTimeMillis() - t0);
        }

        System.out.println("Finished test.");

        if (t != null) {
            t.interrupt();
            t.join();
        }
    }

    /**
     *
     */
    public static void stats() {
        t = new Thread(new Runnable() {
            @SuppressWarnings({"InfiniteLoopStatement", "BusyWait"})
            @Override public void run() {
                int interval = 5;

                while (!Thread.currentThread().isInterrupted()) {
                    long cnt0 = cnt.get();
                    long lt0 = latency.get();

                    try {
                        Thread.sleep(interval * 1000);
                    }
                    catch (InterruptedException e) {
                        System.out.println("Stat thread got interrupted: " + e);

                        return;
                    }

                    long cnt1 = cnt.get();
                    long lt1 = latency.get();

                    System.out.println("Get/s: " + (cnt1 - cnt0) / interval);
                    System.out.println("Avg Latency: " + ((cnt1 - cnt0) > 0 ? (lt1 - lt0) / (cnt1 - cnt0) +
                        "ms" : "invalid"));
                }
            }
        });

        t.start();
    }
}