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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkUtils;

/**
 * Abstract class for Ignite benchmarks which use cache.
 */
public abstract class IgniteCacheAbstractBenchmark<K, V> extends IgniteAbstractBenchmark {
    /** Cache. */
    protected IgniteCache<K, V> cache;

    /** */
    private ThreadLocal<ThreadRange> threadRange = new ThreadLocal<>();

    /** */
    private AtomicInteger threadIdx = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        cache = cache();

        BenchmarkUtils.println(cfg, "Benchmark setUp [name=" + getClass().getSimpleName() +
            ", cacheName="+ cache.getName() +
            ", cacheCfg=" + cache.getConfiguration(CacheConfiguration.class) + ']');

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
