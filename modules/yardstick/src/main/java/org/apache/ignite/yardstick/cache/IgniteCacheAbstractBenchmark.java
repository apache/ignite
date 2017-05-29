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
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.apache.ignite.yardstick.IgniteNode;
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
    protected List<IgniteCache> grpCaches;

    /** */
    private ThreadLocal<ThreadRange> threadRange = new ThreadLocal<>();

    /** */
    private AtomicInteger threadIdx = new AtomicInteger();

    /** */
    private int cachesInGrp;

    /**
     * @return Cache for benchmark operation.
     */
    protected final IgniteCache<K, V> cacheForOperation() {
        if (cachesInGrp > 1)
          return grpCaches.get(ThreadLocalRandom.current().nextInt(cachesInGrp));

        return cache;
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();

        CacheConfiguration<?, ?> ccfg = cache.getConfiguration(CacheConfiguration.class);

        String grpName = ccfg.getGroupName();

        BenchmarkUtils.println(cfg, "Benchmark setUp [name=" + getClass().getSimpleName() +
            ", cacheName="+ cache.getName() +
            ", cacheGroup="+ grpName +
            ", cacheCfg=" + cache.getConfiguration(CacheConfiguration.class) + ']');


        cachesInGrp = args.cachesInGroup();

        if (cachesInGrp > 1) {
            if (grpName == null)
                throw new IllegalArgumentException("Group is not configured for cache: " + cache.getName());

            JdkMarshaller marsh = new JdkMarshaller();

            ccfg = marsh.unmarshal(marsh.marshal(ccfg), null);

            List<CacheConfiguration> toCreate = new ArrayList<>();

            for (int i = 0; i < cachesInGrp - 1; i++) {
                CacheConfiguration<?, ?> ccfg0 = new CacheConfiguration<>(ccfg);

                ccfg0.setName(cache.getName() + "-" + i);
                ccfg0.setGroupName(grpName);

                toCreate.add(ccfg0);
            }

            Collection<IgniteCache> caches = ignite().getOrCreateCaches(toCreate);

            grpCaches = new ArrayList<>(caches);

            grpCaches.add(cache);

            BenchmarkUtils.println(cfg, "Created additional caches for group [cachesInGrp=" + grpCaches.size() +
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

            BenchmarkUtils.println(cfg, "Partition stats. [cacheName: "+ cache.getName() +", topVer: "
                + ignite().cluster().topologyVersion() + "]");
            BenchmarkUtils.println(cfg, "(Node id,  Number of Primary, Percent, Number of Backup, Percent, Total, Percent)");

            for (Map.Entry<ClusterNode, T2<List<Integer>, List<Integer>>> e : parts.entrySet()) {
                List<Integer> primary = e.getValue().get1();
                List<Integer> backup = e.getValue().get2();

                BenchmarkUtils.println(cfg, e.getKey().id() + "  "
                    + primary.size() + "  " + primary.size() * 1. /aff.partitions() + "  "
                    + backup.size() + "  "
                    + backup.size() * 1. / (aff.partitions() * (args.backups() == 0 ? 1 : args.backups())) + "  "
                    + (primary.size() + backup.size()) + "  "
                    + (primary.size() + backup.size() * 1.) / (aff.partitions() * args.backups() + aff.partitions())
                );
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected final void loadCachesData() throws Exception {
        List<IgniteCache> caches = grpCaches != null ? grpCaches : (List)Collections.singletonList(cache);

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
     */
    private void loadCacheData0(String cacheName) {
        println(cfg, "Loading data for cache: " + cacheName);

        long start = System.nanoTime();

        loadCacheData(cacheName);

        println(cfg, "Finished populating data [cache=" + cacheName +
            ", time=" + ((System.nanoTime() - start) / 1_000_000) + "ms]");
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
