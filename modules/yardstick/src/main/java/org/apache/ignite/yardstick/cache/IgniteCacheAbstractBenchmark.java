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

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Abstract class for Ignite benchmarks which use cache.
 */
public abstract class IgniteCacheAbstractBenchmark<K, V> extends IgniteAbstractBenchmark {
    /** Cache. */
    protected IgniteCache<K, V> cache;

    /** */
    protected List<IgniteCache> testCaches;

    /** */
    private ThreadLocal<ThreadRange> threadRange = new ThreadLocal<>();

    /** */
    private AtomicInteger threadIdx = new AtomicInteger();

    /** */
    private int caches;

    /** */
    private final AtomicInteger opCacheIdx = new AtomicInteger();

    /** */
    private final ThreadLocal<IgniteCache<K, V>> opCache = new ThreadLocal<>();

    /**
     * @return Cache for benchmark operation.
     */
    protected final IgniteCache<K, V> cacheForOperation() {
        return cacheForOperation(false);
    }

    /**
     * @param perThread If {@code true} then cache is selected once and set in thread local.
     * @return Cache for benchmark operation.
     */
    protected final IgniteCache<K, V> cacheForOperation(boolean perThread) {
        if (caches > 1) {
            if (perThread) {
                IgniteCache<K, V> cache = opCache.get();

                if (cache == null) {
                    cache = testCaches.get(opCacheIdx.getAndIncrement() % caches);

                    opCache.set(cache);

                    BenchmarkUtils.println(cfg, "Initialized cache for thread [cache=" + cache.getName() + ']');
                }

                return cache;
            }
            else
                return testCaches.get(ThreadLocalRandom.current().nextInt(caches));
        }

        return cache;
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        ignite().cluster().active(true);

        cache = cache();

        CacheConfiguration<?, ?> ccfg = cache.getConfiguration(CacheConfiguration.class);

        String grpName = ccfg.getGroupName();

        BenchmarkUtils.println(cfg, "Benchmark setUp [name=" + getClass().getSimpleName() +
            ", cacheName=" + cache.getName() +
            ", cacheGroup=" + grpName +
            ", cacheCfg=" + cache.getConfiguration(CacheConfiguration.class) + ']');

        caches = args.cachesCount();

        if (caches > 1) {
            List<CacheConfiguration> toCreate = new ArrayList<>();

            for (int i = 0; i < caches - 1; i++) {
                JdkMarshaller marsh = new JdkMarshaller();

                CacheConfiguration ccfg0 = marsh.unmarshal(marsh.marshal(ccfg), null);

                ccfg0.setName(cache.getName() + "-" + i);

                toCreate.add(ccfg0);
            }

            Collection<IgniteCache> caches = ignite().getOrCreateCaches(toCreate);

            testCaches = new ArrayList<>(caches);

            testCaches.add(cache);

            BenchmarkUtils.println(cfg, "Created additional caches [caches=" + testCaches.size() +
                ", grp=" + grpName + ']');
        }

        if (args.printPartitionStatistics()) {
            Map<ClusterNode, T2<List<Integer>, List<Integer>>> parts = new HashMap<>();

            for (ClusterNode node : ignite().cluster().nodes())
                parts.put(node,
                    new T2<List<Integer>, List<Integer>>(new ArrayList<Integer>(), new ArrayList<Integer>()));

            U.sleep(5000);

            Affinity<Object> aff = ignite().affinity(cache.getName());

            for (int p = 0; p < aff.partitions(); p++) {
                Collection<ClusterNode> nodes = aff.mapPartitionToPrimaryAndBackups(p);

                boolean primary = true;

                for (ClusterNode node : nodes) {
                    if (primary) {
                        parts.get(node).get1().add(p);

                        primary = false;
                    }
                    else
                        parts.get(node).get2().add(p);
                }
            }

            BenchmarkUtils.println(cfg, "Partition stats. [cacheName: " + cache.getName() + ", topVer: "
                + ignite().cluster().topologyVersion() + "]");
            BenchmarkUtils.println(cfg, "(Node id,  Number of Primary, Percent, Number of Backup, Percent, Total, Percent)");

            for (Map.Entry<ClusterNode, T2<List<Integer>, List<Integer>>> e : parts.entrySet()) {
                List<Integer> primary = e.getValue().get1();
                List<Integer> backup = e.getValue().get2();

                BenchmarkUtils.println(cfg, e.getKey().id() + "  "
                    + primary.size() + "  " + primary.size() * 1. / aff.partitions() + "  "
                    + backup.size() + "  "
                    + backup.size() * 1. / (aff.partitions() * (args.backups() == 0 ? 1 : args.backups())) + "  "
                    + (primary.size() + backup.size()) + "  "
                    + (primary.size() + backup.size() * 1.) / (aff.partitions() * args.backups() + aff.partitions())
                );
            }
        }

        if (args.enablePreload()) {
            startPreloadLogging(args.preloadLogsInterval());

            preload();

            stopPreloadLogging();
        }
    }

    /**
     * Preload data before benchmarking.
     */
    protected void preload() {
        IgniteSemaphore semaphore = ignite().semaphore("preloadSemaphore",1,true,true);

        semaphore.acquire();

        try {
            IgniteCache<String, Integer> preloadCache = ignite().getOrCreateCache("preloadCache");

            if (preloadCache.get("loaded") == null) {
                IgniteCompute compute = ignite().compute(ignite().cluster().forServers().forOldest());

                IgniteCache<Integer, SampleValue> cache = (IgniteCache<Integer, SampleValue>)cacheForOperation();

                Integer res = compute.apply(new Loader(cache, args, ignite()), 0);

                preloadCache.put("loaded", res);

                if (res != null)
                    args.setRange(res);
            }
            else {
                BenchmarkUtils.println("Setting range to " + preloadCache.get("loaded"));

                args.setRange(preloadCache.get("loaded"));
            }
        }
        finally {
            semaphore.release();
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected final void loadCachesData() throws Exception {
        List<IgniteCache> caches = testCaches != null ? testCaches : Collections.<IgniteCache>singletonList(cache);

        if (caches.size() > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(10);

            try {
                List<Future<?>> futs = new ArrayList<>();

                for (final IgniteCache cache : caches) {
                    futs.add(executor.submit(new Runnable() {
                        @Override public void run() {
                            loadCacheData0(cache.getName());
                        }
                    }));
                }

                for (Future<?> fut : futs)
                    fut.get();
            }
            finally {
                executor.shutdown();
            }
        }
        else
            loadCacheData(caches.get(0).getName());
    }

    /**
     * @param cacheName Cache name.
     * @param cnt Number of entries to load.
     */
    protected final void loadSampleValues(String cacheName, int cnt) {
        try (IgniteDataStreamer<Object, Object> dataLdr = ignite().dataStreamer(cacheName)) {
            for (int i = 0; i < cnt; i++) {
                dataLdr.addData(i, new SampleValue(i));

                if (i % 100000 == 0) {
                    if (Thread.currentThread().isInterrupted())
                        break;

                    println("Loaded entries [cache=" + cacheName + ", cnt=" + i + ']');
                }
            }
        }

        println("Load entries done [cache=" + cacheName + ", cnt=" + cnt + ']');
    }

    /**
     * @param cacheName Cache name.
     */
    private void loadCacheData0(String cacheName) {
        println(cfg, "Loading data for cache: " + cacheName);

        long start = System.nanoTime();

        loadCacheData(cacheName);

        long time = ((System.nanoTime() - start) / 1_000_000);

        println(cfg, "Finished populating data [cache=" + cacheName + ", time=" + time + "ms]");
    }

    /**
     * @param cacheName Cache name.
     */
    protected void loadCacheData(String cacheName) {
        throw new IllegalStateException("Not implemented for " + getClass().getSimpleName());
    }

    /**
     * @return Range.
     */
    protected final ThreadRange threadRange() {
        ThreadRange r = threadRange.get();

        if (r == null) {
            if (args.keysPerThread()) {
                int idx = threadIdx.getAndIncrement();

                int keysPerThread = (int)(args.range() / (float)cfg.threads());

                int min = keysPerThread * idx;
                int max = min + keysPerThread;

                r = new ThreadRange(min, max);
            }
            else
                r = new ThreadRange(0, args.range());

            BenchmarkUtils.println(cfg, "Initialized thread range [min=" + r.min + ", max=" + r.max + ']');

            threadRange.set(r);
        }

        return r;
    }

    /**
     * Each benchmark must determine which cache will be used.
     *
     * @return IgniteCache Cache to use.
     */
    protected abstract IgniteCache<K, V> cache();

    /**
     *
     */
    static class ThreadRange {
        /** */
        final int min;

        /** */
        final int max;

        /** */
        final ThreadLocalRandom rnd;

        /**
         * @param min Min.
         * @param max Max.
         */
        private ThreadRange(int min, int max) {
            this.min = min;
            this.max = max;

            rnd = ThreadLocalRandom.current();
        }

        /**
         * @return Next random key.
         */
        int nextRandom() {
            return rnd.nextInt(min, max);
        }
    }
}
