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

package org.apache.ignite.math.impls;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.functions.*;
import javax.cache.*;
import java.util.*;

/**
 * Distribution-related misc. support.
 */
public class CacheUtils {
    /**
     * TODO: add description.
     * @param <K>
     * @param <V>
     */
    public static class CacheEntry<K, V> {
        private Cache.Entry<K, V> entry;
        private IgniteCache<K, V> cache;

        /**
         *
         * @param entry
         * @param cache
         */
        CacheEntry(Cache.Entry<K, V> entry, IgniteCache<K, V> cache) {
            this.entry = entry;
            this.cache = cache;
        }

        /**
         *
         *
         */
        public Cache.Entry<K, V> entry() {
            return entry;
        }

        /**
         *
         *
         */
        public IgniteCache<K, V> cache() {
            return cache;
        }
    }

    /**
     * Gets local Ignite instance.
     */
    public static Ignite ignite() {
        return Ignition.localIgnite();
    }

    /**
     * 
     * @param cacheName
     * @param k
     * @param <K>
     *
     */
    public static <K> ClusterGroup groupForKey(String cacheName, K k) {
        return ignite().cluster().forNode(ignite().affinity(cacheName).mapKeyToNode(k));
    }

    /**
     *
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param <K>
     * @param <V>
     *
     */
    public static <K, V> double sum(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> subSums = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            if (keyMapper.isValid(ce.entry().getKey())) {
                double v = valMapper.toDouble(ce.entry().getValue());

                return acc == null ? v : acc + v;
            }
            else
                return acc;
        });

        return sum(subSums);
    }

    /**
     *
     * @param cacheName
     *
     */
    public static <K, V> double sparseSum(String cacheName) {
        Collection<Double> subSums = fold(cacheName, (CacheEntry<Integer, Map<Integer, Double>> ce, Double acc) -> {
            Map<Integer, Double> map = ce.entry().getValue();

            double sum = sum(map.values());

            return acc == null ? sum : acc + sum;
        });

        return sum(subSums);
    }

    /**
     *
     * @param c
     *
     */
    private static double sum(Collection<Double> c) {
        double sum = 0.0;

        for (double d : c)
            sum += d;

        return sum;
    }

    /**
     *
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param <K>
     * @param <V>
     *
     */
    public static <K, V> double min(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> mins = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            if (keyMapper.isValid(ce.entry().getKey())) {
                double v = valMapper.toDouble(ce.entry().getValue());

                if (acc == null)
                    return v;
                else
                    return Math.min(acc, v);
            }
            else
                return acc;
        });

        return Collections.min(mins);
    }

    /**
     *
     * @param cacheName
     *
     */
    public static <K, V> double sparseMin(String cacheName) {
        Collection<Double> mins = fold(cacheName, (CacheEntry<Integer, Map<Integer, Double>> ce, Double acc) -> {
            Map<Integer, Double> map = ce.entry().getValue();

            double min = Collections.min(map.values());

            if (acc == null)
                return min;
            else
                return Math.min(acc, min);
        });

        return Collections.min(mins);
    }

    /**
     *
     * @param cacheName
     *
     */
    public static <K, V> double sparseMax(String cacheName) {
        Collection<Double> maxes = fold(cacheName, (CacheEntry<Integer, Map<Integer, Double>> ce, Double acc) -> {
            Map<Integer, Double> map = ce.entry().getValue();

            double max = Collections.max(map.values());

            if (acc == null)
                return max;
            else
                return Math.max(acc, max);
        });

        return Collections.max(maxes);
    }

    /**
     *
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param <K>
     * @param <V>
     *
     */
    public static <K, V> double max(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> maxes = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            if (keyMapper.isValid(ce.entry().getKey())) {
                double v = valMapper.toDouble(ce.entry().getValue());

                if (acc == null)
                    return v;
                else
                    return Math.max(acc, v);
            }
            else
                return acc;
        });

        return Collections.max(maxes);
    }

    /**
     *
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param mapper
     * @param <K>
     * @param <V>
     */
    public static <K, V> void map(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper,
        IgniteFunction<Double, Double> mapper) {
        foreach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            if (keyMapper.isValid(k))
                // Actual assignment.
                ce.cache().put(k, valMapper.fromDouble(mapper.apply(valMapper.toDouble(ce.entry().getValue()))));
        });
    }

    /**
     *
     * @param cacheName
     * @param mapper
     */
    public static <K, V> void sparseMap(String cacheName, IgniteFunction<Double, Double> mapper) {
        foreach(cacheName, (CacheEntry<Integer, Map<Integer, Double>> ce) -> {
            Integer k = ce.entry().getKey();
            Map<Integer, Double> v = ce.entry().getValue();

            for (Map.Entry<Integer, Double> e : v.entrySet())
                e.setValue(mapper.apply(e.getValue()));

            ce.cache().put(k, v);
        });
    }

    /**
     * 
     * @param cacheName
     * @param fun
     * @param <K>
     * @param <V>
     */
    public static <K, V> void foreach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun) {
        bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for scan query. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode localNode = ignite.cluster().localNode();

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++){
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                    (k, v) -> affinity.mapPartitionToNode(p) == localNode)))
                    fun.accept(new CacheEntry<K, V>(entry, cache));
            }
        });
    }

    /**
     * <b>Currently fold supports only commutative operations.<b/>
     * @param cacheName
     * @param folder
     * @param <K>
     * @param <V>
     * @param <A>
     *
     */
    public static <K, V, A> Collection<A> fold(String cacheName, IgniteBiFunction<CacheEntry<K, V>, A, A> folder) {
        return bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for ScanQuery. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode localNode = ignite.cluster().localNode();

            A a = null;

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++) {
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                    (k, v) -> affinity.mapPartitionToNode(p) == localNode)))
                    a = folder.apply(new CacheEntry<K, V>(entry, cache), a);
            }

            return a;
        });
    }

    /**
     *
     * @param cacheName
     * @param run
     */
    public static void bcast(String cacheName, IgniteRunnable run) {
        ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(run);
    }

    /**
     * 
     * @param cacheName
     * @param call
     * @param <A>
     *
     */
    public static <A> Collection<A> bcast(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }
}
