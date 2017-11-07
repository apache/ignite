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

package org.apache.ignite.ml.math.distributed;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.BinaryOperator;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.ml.math.KeyMapper;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey;
import org.apache.ignite.ml.math.distributed.keys.impl.VectorBlockKey;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.MatrixBlockEntry;
import org.apache.ignite.ml.math.impls.vector.VectorBlockEntry;

/**
 * Distribution-related misc. support.
 *
 * TODO: IGNITE-5102, fix sparse key filters.
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
    protected static <K> ClusterGroup getClusterGroupForGivenKey(String cacheName, K k) {
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
     * @param matrixUuid Matrix UUID.
     * @return Sum obtained using sparse logic.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> double sparseSum(IgniteUuid matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> subSums = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double sum;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>)v;

                sum = sum(map.values());
            }
            else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry)v;

                sum = be.sum();
            }
            else
                throw new UnsupportedOperationException();

            return acc == null ? sum : acc + sum;
        }, sparseKeyFilter(matrixUuid));

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
     * @param matrixUuid Matrix UUID.
     * @return Minimum value obtained using sparse logic.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> double sparseMin(IgniteUuid matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> mins = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double min;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>)v;

                min = Collections.min(map.values());
            }
            else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry)v;

                min = be.minValue();
                System.out.println("min " + min);
            }
            else
                throw new UnsupportedOperationException();

            if (acc == null)
                return min;
            else
                return Math.min(acc, min);

        }, sparseKeyFilter(matrixUuid));

        return Collections.min(mins);
    }

    /**
     * @param matrixUuid Matrix UUID.
     * @return Maximum value obtained using sparse logic.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> double sparseMax(IgniteUuid matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> maxes = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double max;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>)v;

                max = Collections.max(map.values());
            }
            else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry)v;

                max = be.maxValue();
            }
            else
                throw new UnsupportedOperationException();

            if (acc == null)
                return max;
            else
                return Math.max(acc, max);

        }, sparseKeyFilter(matrixUuid));

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
     * @param matrixUuid Matrix UUID.
     * @param mapper Mapping {@link IgniteFunction}.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> void sparseMap(IgniteUuid matrixUuid, IgniteDoubleFunction<Double> mapper, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");
        A.notNull(mapper, "mapper");

        foreach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            V v = ce.entry().getValue();

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>)v;

                for (Map.Entry<Integer, Double> e : (map.entrySet()))
                    e.setValue(mapper.apply(e.getValue()));

            }
            else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry)v;

                be.map(mapper);
            }
            else
                throw new UnsupportedOperationException();

            ce.cache().put(k, v);
        }, sparseKeyFilter(matrixUuid));
    }

    /**
     * Filter for distributed matrix keys.
     *
     * @param matrixUuid Matrix uuid.
     */
    protected static <K> IgnitePredicate<K> sparseKeyFilter(IgniteUuid matrixUuid) {
        return key -> {
            if (key instanceof MatrixBlockKey)
                return ((MatrixBlockKey)key).dataStructureId().equals(matrixUuid);
            else if (key instanceof RowColMatrixKey)
                return ((RowColMatrixKey)key).dataStructureId().equals(matrixUuid);
            else if (key instanceof VectorBlockKey)
                return ((VectorBlockKey)key).dataStructureId().equals(matrixUuid);
            else
                throw new UnsupportedOperationException(); // TODO: handle my poor doubles
        };
    }

    /**
     * @param cacheName Cache name.
     * @param fun An operation that accepts a cache entry and processes it.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     */
    private static <K, V> void foreach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun) {
        foreach(cacheName, fun, null);
    }

    /**
     * @param cacheName Cache name.
     * @param fun An operation that accepts a cache entry and processes it.
     * @param keyFilter Cache keys filter.
     * @param <K> Cache key object type.
     * @param <V> Cache value object type.
     */
    protected static <K, V> void foreach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun,
                                         IgnitePredicate<K> keyFilter) {
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
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part, (k, v) -> affinity.mapPartitionToNode(p) == locNode && (keyFilter == null || keyFilter.apply(k)))))
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
        return fold(cacheName, folder, null);
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
    public static <K, V, A> Collection<A> fold(String cacheName, IgniteBiFunction<CacheEntry<K, V>, A, A> folder,
        IgnitePredicate<K> keyFilter) {
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
                    (k, v) -> affinity.mapPartitionToNode(p) == locNode && (keyFilter == null || keyFilter.apply(k))))){
                    a = folder.apply(new CacheEntry<>(entry, cache), a);
                    System.out.println("a " + a);
                }


            }

            return a;
        });
    }

    /**
     * Distributed version of fold operation.
     *
     * @param cacheName Cache name.
     * @param folder Folder.
     * @param keyFilter Key filter.
     * @param accumulator Accumulator.
     * @param zeroVal Zero value.
     */
    public static <K, V, A> A distributedFold(String cacheName, IgniteBiFunction<Cache.Entry<K, V>, A, A> folder,
        IgnitePredicate<K> keyFilter, BinaryOperator<A> accumulator, A zeroVal) {
        return sparseFold(cacheName, folder, keyFilter, accumulator, zeroVal, null, null, 0,
            false);
    }

    /**
     * Sparse version of fold. This method also applicable to sparse zeroes.
     *
     * @param cacheName Cache name.
     * @param folder Folder.
     * @param keyFilter Key filter.
     * @param accumulator Accumulator.
     * @param zeroVal Zero value.
     * @param defVal Def value.
     * @param defKey Def key.
     * @param defValCnt Def value count.
     * @param isNilpotent Is nilpotent.
     */
    private static <K, V, A> A sparseFold(String cacheName, IgniteBiFunction<Cache.Entry<K, V>, A, A> folder,
        IgnitePredicate<K> keyFilter, BinaryOperator<A> accumulator, A zeroVal, V defVal, K defKey, long defValCnt,
        boolean isNilpotent) {

        A defRes = zeroVal;

        if (!isNilpotent)
            for (int i = 0; i < defValCnt; i++)
                defRes = folder.apply(new CacheEntryImpl<>(defKey, defVal), defRes);

        Collection<A> totalRes = bcast(cacheName, () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<K, V> cache = ignite.getOrCreateCache(cacheName);

            int partsCnt = ignite.affinity(cacheName).partitions();

            // Use affinity in filter for ScanQuery. Otherwise we accept consumer in each node which is wrong.
            Affinity affinity = ignite.affinity(cacheName);
            ClusterNode locNode = ignite.cluster().localNode();

            A a = zeroVal;

            // Iterate over all partitions. Some of them will be stored on that local node.
            for (int part = 0; part < partsCnt; part++) {
                int p = part;

                // Iterate over given partition.
                // Query returns an empty cursor if this partition is not stored on this node.
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                    (k, v) -> affinity.mapPartitionToNode(p) == locNode && (keyFilter == null || keyFilter.apply(k)))))
                    a = folder.apply(entry, a);
            }

            return a;
        });
        totalRes.add(defRes);
        return totalRes.stream().reduce(zeroVal, accumulator);
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
    private static <A> Collection<A> bcast(String cacheName, IgniteCallable<A> call) {
        return ignite().compute(ignite().cluster().forCacheNodes(cacheName)).broadcast(call);
    }

    /**
     * @param vectorUuid Matrix UUID.
     * @param mapper Mapping {@link IgniteFunction}.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> void sparseMapForVector(IgniteUuid vectorUuid, IgniteDoubleFunction<V> mapper, String cacheName) {
        A.notNull(vectorUuid, "vectorUuid");
        A.notNull(cacheName, "cacheName");
        A.notNull(mapper, "mapper");

        foreach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            V v = ce.entry().getValue();

            if (v instanceof VectorBlockEntry){
                VectorBlockEntry entry = (VectorBlockEntry)v;

                for (int i = 0; i < entry.size(); i++) entry.set(i, (Double) mapper.apply(entry.get(i)));

                ce.cache().put(k, (V) entry);
            } else {
                V mappingRes = mapper.apply((Double)v);
                ce.cache().put(k, mappingRes);
            }
        }, sparseKeyFilter(vectorUuid));
    }
}
