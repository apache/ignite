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

package org.apache.ignite.ml.math.impls;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.ml.math.KeyMapper;
import org.apache.ignite.ml.math.ValueMapper;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Distribution-related misc. support.
 */
public class CacheUtils {
    /**
     * Cache entry support.
     *
     * @param <K>
     * @param <V>
     */
    public static class CacheEntry<K, V> {
        /** */
        private Cache.Entry<K, V> entry;
        /** */
        private IgniteCache<K, V> cache;

        /**
         * @param entry Original cache entry.
         * @param cache Cache instance.
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
     * @param cacheName Cache name.
     * @param k Key into the cache.
     * @param <K> Key type.
     * @return Cluster group for given key.
     */
    public static <K> ClusterGroup groupForKey(String cacheName, K k) {
        return ignite().cluster().forNode(ignite().affinity(cacheName).mapKeyToNode(k));
    }

    /**
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     * @return Sum of the values obtained for valid keys.
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
     * @param cacheName Cache name.
     * @return Sum obtained using sparse logic.
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
     * @param c {@link Collection} of double values to sum.
     * @return Sum of the values.
     */
    private static double sum(Collection<Double> c) {
        double sum = 0.0;

        for (double d : c)
            sum += d;

        return sum;
    }

    /**
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     * @return Minimum value for valid keys.
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
     * @param cacheName Cache name.
     * @return Minimum value obtained using sparse logic.
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
     * @param cacheName Cache name.
     * @return Maximum value obtained using sparse logic.
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
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     * @return Maximum value for valid keys.
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
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param mapper Mapping {@link IgniteFunction}.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
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
     * @param cacheName Cache name.
     * @param mapper Mapping {@link IgniteFunction}.
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
     * @param cacheName Cache name.
     * @param fun An operation that accepts a cache entry and processes it.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     */
    public static <K, V> void foreach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun) {
        bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for scan query. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++) {
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                    (k, v) -> affinity.mapPartitionToNode(p) == locNode)))
                    fun.accept(new CacheEntry<>(entry, cache));
            }
        });
    }

    /**
     * <b>Currently fold supports only commutative operations.<b/>
     *
     * @param cacheName Cache name.
     * @param folder Fold function operating over cache entries.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     * @param <A> Fold result type.
     * @return Fold operation result.
     */
    public static <K, V, A> Collection<A> fold(String cacheName, IgniteBiFunction<CacheEntry<K, V>, A, A> folder) {
        return bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for ScanQuery. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            A a = null;

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++) {
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                    (k, v) -> affinity.mapPartitionToNode(p) == locNode)))
                    a = folder.apply(new CacheEntry<>(entry, cache), a);
            }

            return a;
        });
    }

    /**
     * @param cacheName Cache name.
     * @param run {@link Runnable} to broadcast to cache nodes for given cache name.
     */
    public static void bcast(String cacheName, IgniteRunnable run) {
        ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(run);
    }

    /**
     * @param cacheName Cache name.
     * @param call {@link IgniteCallable} to broadcast to cache nodes for given cache name.
     * @param <A> Type returned by the callable.
     */
    public static <A> Collection<A> bcast(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }
}
