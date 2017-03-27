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
import javax.cache.*;
import java.util.*;
import org.apache.ignite.math.KeyMapper;
import org.apache.ignite.math.ValueMapper;
import org.apache.ignite.math.functions.IgniteBiFunction;
import org.apache.ignite.math.functions.IgniteConsumer;
import org.apache.ignite.math.functions.IgniteFunction;

/**
 * Distribution-related misc. support.
 */
public class CacheSupport {
    /**
     *
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
         * @return
         */
        public Cache.Entry<K, V> entry() {
            return entry;
        }

        /**
         *
         * @return
         */
        public IgniteCache<K, V> cache() {
            return cache;
        }
    }

    /**
     * Gets local Ignite instance.
     */
    protected Ignite ignite() {
        return Ignition.localIgnite();
    }

    /**
     *
     * @param cacheName
     * @param run
     */
    protected void broadcastForCache(String cacheName, IgniteRunnable run) {
        ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(run);
    }

    /**
     * 
     * @param cacheName
     * @param k
     * @param <K>
     * @return
     */
    protected <K> ClusterGroup groupForKey(String cacheName, K k) {
        return ignite().cluster().forNode(ignite().affinity(cacheName).mapKeyToNode(k));
    }

    /**
     *
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param <K>
     * @param <V>
     * @return
     */
    protected <K, V> double cacheSum(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> subSums = cacheFold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            if (keyMapper.isValid(ce.entry().getKey())) {
                double v = valMapper.toDouble(ce.entry().getValue());

                return acc == null ? v : acc + v;
            }
            else
                return acc;
        });

        double sum = 0.0;

        for (double d : subSums)
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
     * @return
     */
    protected <K, V> double cacheMin(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> mins = cacheFold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
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
     * @param keyMapper
     * @param valMapper
     * @param <K>
     * @param <V>
     * @return
     */
    protected <K, V> double cacheMax(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> maxes = cacheFold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
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
     * s
     * @param cacheName
     * @param keyMapper
     * @param valMapper
     * @param mapper
     * @param <K>
     * @param <V>
     */
    protected <K, V> void cacheMap(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper, IgniteFunction<Double, Double> mapper) {
        cacheForeach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            if (keyMapper.isValid(k))
                // Actual assignment.
                ce.cache().put(k, valMapper.fromDouble(mapper.apply(valMapper.toDouble(ce.entry().getValue()))));
        });
    }

    /**
     * 
     * @param cacheName
     * @param fun
     * @param <K>
     * @param <V>
     */
    protected <K, V> void cacheForeach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun) {
        broadcastForCache(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for ScanQuery. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode localNode = ignite.cluster().localNode();

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++){
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part, (k, v) -> affinity.mapPartitionToNode(p) == localNode)))
                    fun.accept(new CacheEntry<K, V>(entry, cache));
            }
        });
    }

    /**
     *
     * @param cacheName
     * @param folder
     * @param <K>
     * @param <V>
     * @param <A>
     * @return
     */
    protected <K, V, A> Collection<A> cacheFold(String cacheName, IgniteBiFunction<CacheEntry<K, V>, A, A> folder) {
        return broadcastForCache(cacheName, () -> {
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
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part, (k, v) -> affinity.mapPartitionToNode(p) == localNode)))
                    a = folder.apply(new CacheEntry<K, V>(entry, cache), a);
            }

            return a;
        });
    }

    /**
     * 
     * @param cacheName
     * @param call
     * @param <A>
     * @return
     */
    protected <A> Collection<A> broadcastForCache(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }
}
