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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.ml.math.KeyMapper;
import org.apache.ignite.ml.math.distributed.keys.DataStructureCacheKey;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.distributed.keys.impl.MatrixBlockKey;
import org.apache.ignite.ml.math.distributed.keys.impl.VectorBlockKey;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.*;
import org.apache.ignite.ml.math.impls.matrix.MatrixBlockEntry;
import org.apache.ignite.ml.math.impls.vector.VectorBlockEntry;

import javax.cache.Cache;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

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
         */
        public Cache.Entry<K, V> entry() {
            return entry;
        }

        /**
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
     * @param k         Key into the cache.
     * @param <K>       Key type.
     * @return Cluster group for given key.
     */
    protected static <K> ClusterGroup getClusterGroupForGivenKey(String cacheName, K k) {
        return ignite().cluster().forNode(ignite().affinity(cacheName).mapKeyToNode(k));
    }

    /**
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     * @return Sum of the values obtained for valid keys.
     */
    public static <K, V> double sum(String cacheName, KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        Collection<Double> subSums = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            if (keyMapper.isValid(ce.entry().getKey())) {
                double v = valMapper.toDouble(ce.entry().getValue());

                return acc == null ? v : acc + v;
            } else
                return acc;
        });

        return sum(subSums);
    }

    /**
     * @param matrixUuid Matrix UUID.
     * @return Sum obtained using sparse logic.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> double sparseSum(UUID matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> subSums = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double sum;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>) v;

                sum = sum(map.values());
            } else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry) v;

                sum = be.sum();
            } else
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
        // Fix for IGNITE-6762, some collections could store null values.
        return c.stream().filter(Objects::nonNull).mapToDouble(Double::doubleValue).sum();
    }

    /**
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
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
            } else
                return acc;
        });

        return Collections.min(mins);
    }

    /**
     * @param matrixUuid Matrix UUID.
     * @return Minimum value obtained using sparse logic.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> double sparseMin(UUID matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> mins = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double min;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>) v;

                min = Collections.min(map.values());
            } else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry) v;

                min = be.minValue();
            } else
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
    public static <K, V> double sparseMax(UUID matrixUuid, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");

        Collection<Double> maxes = fold(cacheName, (CacheEntry<K, V> ce, Double acc) -> {
            V v = ce.entry().getValue();

            double max;

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>) v;

                max = Collections.max(map.values());
            } else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry) v;

                max = be.maxValue();
            } else
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
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
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
            } else
                return acc;
        });

        return Collections.max(maxes);
    }

    /**
     * @param cacheName Cache name.
     * @param keyMapper {@link KeyMapper} to validate cache key.
     * @param valMapper {@link ValueMapper} to obtain double value for given cache key.
     * @param mapper    Mapping {@link IgniteFunction}.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
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
     * @param mapper     Mapping {@link IgniteFunction}.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> void sparseMap(UUID matrixUuid, IgniteDoubleFunction<Double> mapper, String cacheName) {
        A.notNull(matrixUuid, "matrixUuid");
        A.notNull(cacheName, "cacheName");
        A.notNull(mapper, "mapper");

        foreach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            V v = ce.entry().getValue();

            if (v instanceof Map) {
                Map<Integer, Double> map = (Map<Integer, Double>) v;

                for (Map.Entry<Integer, Double> e : (map.entrySet()))
                    e.setValue(mapper.apply(e.getValue()));

            } else if (v instanceof MatrixBlockEntry) {
                MatrixBlockEntry be = (MatrixBlockEntry) v;

                be.map(mapper);
            } else
                throw new UnsupportedOperationException();

            ce.cache().put(k, v);
        }, sparseKeyFilter(matrixUuid));
    }

    /**
     * Filter for distributed matrix keys.
     *
     * @param matrixUuid Matrix uuid.
     */
    private static <K> IgnitePredicate<K> sparseKeyFilter(UUID matrixUuid) {
        return key -> {
            if (key instanceof DataStructureCacheKey)
                return ((DataStructureCacheKey) key).dataStructureId().equals(matrixUuid);
            else if (key instanceof IgniteBiTuple)
                return ((IgniteBiTuple<Integer, UUID>) key).get2().equals(matrixUuid);
            else if (key instanceof MatrixBlockKey)
                return ((MatrixBlockKey) key).dataStructureId().equals(matrixUuid);
            else if (key instanceof RowColMatrixKey)
                return ((RowColMatrixKey) key).dataStructureId().equals(matrixUuid);
            else if (key instanceof VectorBlockKey)
                return ((VectorBlockKey) key).dataStructureId().equals(matrixUuid);
            else
                throw new UnsupportedOperationException(); // TODO: handle my poor doubles
        };
    }

    /**
     * @param cacheName Cache name.
     * @param fun       An operation that accepts a cache entry and processes it.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     */
    private static <K, V> void foreach(String cacheName, IgniteConsumer<CacheEntry<K, V>> fun) {
        foreach(cacheName, fun, null);
    }

    /**
     * @param cacheName Cache name.
     * @param fun       An operation that accepts a cache entry and processes it.
     * @param keyFilter Cache keys filter.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
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
                for (Cache.Entry<K, V> entry : cache.query(new ScanQuery<K, V>(part,
                        (k, v) -> affinity.mapPartitionToNode(p) == locNode && (keyFilter == null || keyFilter.apply(k)))))
                    fun.accept(new CacheEntry<>(entry, cache));
            }
        });
    }

    /**
     * @param cacheName Cache name.
     * @param fun       An operation that accepts a cache entry and processes it.
     * @param ignite    Ignite.
     * @param keysGen   Keys generator.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     */
    public static <K, V> void update(String cacheName, Ignite ignite,
                                     IgniteBiFunction<Ignite, Cache.Entry<K, V>, Stream<Cache.Entry<K, V>>> fun, IgniteSupplier<Set<K>> keysGen) {
        bcast(cacheName, ignite, () -> {
            Ignite ig = Ignition.localIgnite();
            IgniteCache<K, V> cache = ig.getOrCreateCache(cacheName);

            Affinity<K> affinity = ig.affinity(cacheName);
            ClusterNode locNode = ig.cluster().localNode();

            Collection<K> ks = affinity.mapKeysToNodes(keysGen.get()).get(locNode);

            if (ks == null)
                return;

            Map<K, V> m = new ConcurrentHashMap<>();

            ks.parallelStream().forEach(k -> {
                V v = cache.localPeek(k);
                if (v != null)
                    (fun.apply(ignite, new CacheEntryImpl<>(k, v))).forEach(ent -> m.put(ent.getKey(), ent.getValue()));
            });

            cache.putAll(m);
        });
    }

    /**
     * @param cacheName Cache name.
     * @param fun       An operation that accepts a cache entry and processes it.
     * @param ignite    Ignite.
     * @param keysGen   Keys generator.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     */
    public static <K, V> void update(String cacheName, Ignite ignite, IgniteConsumer<Cache.Entry<K, V>> fun,
                                     IgniteSupplier<Set<K>> keysGen) {
        bcast(cacheName, ignite, () -> {
            Ignite ig = Ignition.localIgnite();
            IgniteCache<K, V> cache = ig.getOrCreateCache(cacheName);

            Affinity<K> affinity = ig.affinity(cacheName);
            ClusterNode locNode = ig.cluster().localNode();

            Collection<K> ks = affinity.mapKeysToNodes(keysGen.get()).get(locNode);

            if (ks == null)
                return;

            Map<K, V> m = new ConcurrentHashMap<>();

            for (K k : ks) {
                V v = cache.localPeek(k);
                fun.accept(new CacheEntryImpl<>(k, v));
                m.put(k, v);
            }

            long before = System.currentTimeMillis();
            cache.putAll(m);
            System.out.println("PutAll took: " + (System.currentTimeMillis() - before));
        });
    }

    /**
     * <b>Currently fold supports only commutative operations.<b/>
     *
     * @param cacheName Cache name.
     * @param folder    Fold function operating over cache entries.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     * @param <A>       Fold result type.
     * @return Fold operation result.
     */
    public static <K, V, A> Collection<A> fold(String cacheName, IgniteBiFunction<CacheEntry<K, V>, A, A> folder) {
        return fold(cacheName, folder, null);
    }

    /**
     * <b>Currently fold supports only commutative operations.<b/>
     *
     * @param cacheName Cache name.
     * @param folder    Fold function operating over cache entries.
     * @param <K>       Cache key object type.
     * @param <V>       Cache value object type.
     * @param <A>       Fold result type.
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
                        (k, v) -> affinity.mapPartitionToNode(p) == locNode && (keyFilter == null || keyFilter.apply(k)))))
                    a = folder.apply(new CacheEntry<>(entry, cache), a);
            }

            return a;
        });
    }

    /**
     * Distributed version of fold operation.
     *
     * @param cacheName   Cache name.
     * @param folder      Folder.
     * @param keyFilter   Key filter.
     * @param accumulator Accumulator.
     * @param zeroValSupp Zero value supplier.
     */
    public static <K, V, A> A distributedFold(String cacheName, IgniteBiFunction<Cache.Entry<K, V>, A, A> folder,
                                              IgnitePredicate<K> keyFilter, BinaryOperator<A> accumulator, IgniteSupplier<A> zeroValSupp) {
        return sparseFold(cacheName, folder, keyFilter, accumulator, zeroValSupp, null, null, 0,
                false);
    }

    /**
     * Sparse version of fold. This method also applicable to sparse zeroes.
     *
     * @param cacheName   Cache name.
     * @param folder      Folder.
     * @param keyFilter   Key filter.
     * @param accumulator Accumulator.
     * @param zeroValSupp Zero value supplier.
     * @param defVal      Default value.
     * @param defKey      Default key.
     * @param defValCnt   Def value count.
     * @param isNilpotent Is nilpotent.
     */
    private static <K, V, A> A sparseFold(String cacheName, IgniteBiFunction<Cache.Entry<K, V>, A, A> folder,
                                          IgnitePredicate<K> keyFilter, BinaryOperator<A> accumulator, IgniteSupplier<A> zeroValSupp, V defVal, K defKey,
                                          long defValCnt, boolean isNilpotent) {

        A defRes = zeroValSupp.get();

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

            A a = zeroValSupp.get();

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
        return totalRes.stream().reduce(defRes, accumulator);
    }

    public static <K, V, A, W> A reduce(String cacheName, Ignite ignite,
                                        IgniteTriFunction<W, Cache.Entry<K, V>, A, A> acc,
                                        IgniteSupplier<W> supp,
                                        IgniteSupplier<Iterable<Cache.Entry<K, V>>> entriesGen, IgniteBinaryOperator<A> comb,
                                        IgniteSupplier<A> zeroValSupp) {

        A defRes = zeroValSupp.get();

        Collection<A> totalRes = bcast(cacheName, ignite, () -> {
            // Use affinity in filter for ScanQuery. Otherwise we accept consumer in each node which is wrong.
            A a = zeroValSupp.get();

            W w = supp.get();

            for (Cache.Entry<K, V> kvEntry : entriesGen.get())
                a = acc.apply(w, kvEntry, a);

            return a;
        });

        return totalRes.stream().reduce(defRes, comb);
    }

    public static <K, V, A, W> A reduce(String cacheName, IgniteTriFunction<W, Cache.Entry<K, V>, A, A> acc,
                                        IgniteSupplier<W> supp,
                                        IgniteSupplier<Iterable<Cache.Entry<K, V>>> entriesGen, IgniteBinaryOperator<A> comb,
                                        IgniteSupplier<A> zeroValSupp) {
        return reduce(cacheName, Ignition.localIgnite(), acc, supp, entriesGen, comb, zeroValSupp);
    }

    /**
     * @param cacheName Cache name.
     * @param run       {@link Runnable} to broadcast to cache nodes for given cache name.
     */
    public static void bcast(String cacheName, Ignite ignite, IgniteRunnable run) {
        ignite.compute(ignite.cluster().forDataNodes(cacheName)).broadcast(run);
    }

    /**
     * Broadcast runnable to data nodes of given cache.
     *
     * @param cacheName Cache name.
     * @param run       Runnable.
     */
    public static void bcast(String cacheName, IgniteRunnable run) {
        bcast(cacheName, ignite(), run);
    }

    /**
     * @param cacheName Cache name.
     * @param call      {@link IgniteCallable} to broadcast to cache nodes for given cache name.
     * @param <A>       Type returned by the callable.
     */
    public static <A> Collection<A> bcast(String cacheName, IgniteCallable<A> call) {
        return bcast(cacheName, ignite(), call);
    }

    /**
     * Broadcast callable to data nodes of given cache.
     *
     * @param cacheName Cache name.
     * @param ignite    Ignite instance.
     * @param call      Callable to broadcast.
     * @param <A>       Type of callable result.
     * @return Results of callable from each node.
     */
    public static <A> Collection<A> bcast(String cacheName, Ignite ignite, IgniteCallable<A> call) {
        return ignite.compute(ignite.cluster().forDataNodes(cacheName)).broadcast(call);
    }

    /**
     * @param vectorUuid Matrix UUID.
     * @param mapper     Mapping {@link IgniteFunction}.
     */
    @SuppressWarnings("unchecked")
    public static <K, V> void sparseMapForVector(UUID vectorUuid, IgniteDoubleFunction<V> mapper, String cacheName) {
        A.notNull(vectorUuid, "vectorUuid");
        A.notNull(cacheName, "cacheName");
        A.notNull(mapper, "mapper");

        foreach(cacheName, (CacheEntry<K, V> ce) -> {
            K k = ce.entry().getKey();

            V v = ce.entry().getValue();

            if (v instanceof VectorBlockEntry) {
                VectorBlockEntry entry = (VectorBlockEntry) v;

                for (int i = 0; i < entry.size(); i++) entry.set(i, (Double) mapper.apply(entry.get(i)));

                ce.cache().put(k, (V) entry);
            } else {
                V mappingRes = mapper.apply((Double) v);
                ce.cache().put(k, mappingRes);
            }
        }, sparseKeyFilter(vectorUuid));
    }
}
