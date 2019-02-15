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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Benchmark created to verify that slow EntryProcessor does not affect 'get' performance.
 */
public class IgniteGetFromComputeBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private static final String CACHE_NAME = "atomic";

    /** */
    private IgniteCompute compute;

    /** */
    private IgniteCache asyncCache;

    /** */
    private ThreadLocal<IgniteFuture> invokeFut = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (args.preloadAmount() > args.range())
            throw new IllegalArgumentException("Preloading amount (\"-pa\", \"--preloadAmount\") " +
                "must by less then the range (\"-r\", \"--range\").");

        String cacheName = cache().getName();

        println(cfg, "Loading data for cache: " + cacheName);

        long start = System.nanoTime();

        try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cacheName)) {
            for (int i = 0; i < args.preloadAmount(); i++) {
                dataLdr.addData(i, new SampleValue(i));

                if (i % 100000 == 0) {
                    if (Thread.currentThread().isInterrupted())
                        break;

                    println("Loaded entries: " + i);
                }
            }
        }

        println(cfg, "Finished populating data [time=" + ((System.nanoTime() - start) / 1_000_000) + "ms, " +
            "amount=" + args.preloadAmount() + ']');

        compute = ignite().compute();

        asyncCache = cache().withAsync();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteFuture fut = invokeFut.get();

        if (fut == null || fut.isDone()) {
            Set<Integer> keys = new TreeSet<>();

            for (int i = 0; i < 3; i++)
                keys.add(nextRandom(args.range()));

            asyncCache.invokeAll(keys, new SlowEntryProcessor(0));

            invokeFut.set(asyncCache.future());
        }

        int key = nextRandom(args.range());

        compute.affinityCall(CACHE_NAME, key, new GetClosure(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache(CACHE_NAME);
    }

    /**
     *
     */
    public static class GetClosure implements IgniteCallable<Object> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final int key;

        /**
         * @param key Key.
         */
        public GetClosure(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            return ignite.cache(CACHE_NAME).get(key);
        }
    }

    /**
     *
     */
    public static class SlowEntryProcessor implements CacheEntryProcessor<Integer, Object, Object> {
        /** */
        private Object val;

        /**
         * @param val Value.
         */
        public SlowEntryProcessor(Object val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Object> entry, Object... args) {
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException ignore) {
                // No-op.
            }

            entry.setValue(val);

            return null;
        }
    }
}
