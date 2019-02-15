/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 *
 */
public class IntMaxValueEntriesTest extends IgniteCacheAbstractBenchmark {
    /** Threads. */
    private static final int THREADS = 16;

    /** Keys lo. */
    private static final int KEYS_LO = -100_000;

    /** Keys hi. */
    private static final long KEYS_HI = Integer.MAX_VALUE;

    /** Report delta. */
    private static final int REPORT_DELTA = 1_000_000;

    /** Cache name. */
    private static final String CACHE_NAME = "int-max-value-cache";

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final IgniteCache<Integer, Object> cache = cache();

        final IgniteDataStreamer<Integer, Object> stmr = ignite().dataStreamer(cache.getName());

        final List<Thread> threads = new ArrayList<>(THREADS);

        final LongAdder addedCnt = new LongAdder();

        int delta = (int)((KEYS_HI + Math.abs(KEYS_LO)) / THREADS);

        System.out.println("Delta: " + delta);

        for (int i = 0; i < THREADS; i++) {
            final int lo = i == 0 ? KEYS_LO : delta * i + 1;

            final int hi = i == THREADS - 1 ? (int)KEYS_HI : (int)((long)delta * (i + 1));

            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    byte val = (byte)rnd.nextInt();

                    println("Start from " + lo + " to " + hi);

                    for (int j = lo, k = 0; j < hi; j++, k++) {
                        stmr.addData(j, val++);

                        addedCnt.increment();

                        if (k % REPORT_DELTA == 0)
                            println(addedCnt.sum() + " entries");
                    }

                    println("Thread finished. " + addedCnt.sum() + " entries.");
                }
            });

            threads.add(t);
            t.start();
        }

        for (Thread thread : threads)
            thread.join();

        println("All threads finished. " + addedCnt.sum() + " entries.");

        println("Streamer flush");

        stmr.flush();

        println("Streamer flushed");

        println("Calculating cache size");
        println("Cache size: " + cache.size());

        println("Calculating long cache size");
        println("Cache size long: " + cache.sizeLong());

        Thread.sleep(10000);

        println("Iterating started");

        long cnt = 0;

        for (Cache.Entry<Integer, Object> ignored : cache) {
            cnt++;

            if (cnt > 0 && cnt % REPORT_DELTA == 0)
                println("Iterated via " + cnt + " entries");
        }

        println("Iterated via " + cnt + " entries");

        cache.destroy();

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache cache() {
        return ignite().cache(CACHE_NAME);
    }
}
