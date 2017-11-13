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

package org.apache.ignite.ml.trees.trainers.columnbased;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.cache.CacheEntryImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/**
 * Adapter of a given cache to {@see CacheColumnDecisionTreeTrainerInput}
 *
 * @param <K> Class of keys of the cache.
 * @param <V> Class of values of the cache.
 */
public abstract class CacheColumnDecisionTreeTrainerInput<K, V> implements ColumnDecisionTreeTrainerInput {
    /** Supplier of labels key. */
    private final IgniteSupplier<Stream<K>> labelsKeys;

    /** Count of features. */
    private final int featuresCnt;

    /** Function which maps feature index to Stream of keys corresponding to this feature index. */
    private final IgniteFunction<Integer, Stream<K>> keyMapper;

    /** Information about which features are categorical in form of feature index -> number of categories. */
    private final Map<Integer, Integer> catFeaturesInfo;

    /** Cache name. */
    private final String cacheName;

    /** Count of samples. */
    private final int samplesCnt;

    /** Function used for mapping cache values to stream of tuples. */
    private final IgniteFunction<Cache.Entry<K, V>, Stream<IgniteBiTuple<Integer, Double>>> valuesMapper;

    /**
     * Function which map value of entry with label key to DoubleStream.
     * Look at {@code CacheColumnDecisionTreeTrainerInput::labels} for understanding how {@code labelsKeys} and
     * {@code labelsMapper} interact.
     */
    private final IgniteFunction<V, DoubleStream> labelsMapper;

    /**
     * Constructs input for {@see org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer}.
     *
     * @param c Cache.
     * @param valuesMapper Function for mapping cache entry to stream used by {@link
     * org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer}.
     * @param labelsMapper Function used for mapping cache value to labels array.
     * @param keyMapper Function used for mapping feature index to the cache key.
     * @param catFeaturesInfo Information about which features are categorical in form of feature index -> number of
     * categories.
     * @param featuresCnt Count of features.
     * @param samplesCnt Count of samples.
     */
    // TODO: IGNITE-5724 think about boxing/unboxing
    public CacheColumnDecisionTreeTrainerInput(IgniteCache<K, V> c,
        IgniteSupplier<Stream<K>> labelsKeys,
        IgniteFunction<Cache.Entry<K, V>, Stream<IgniteBiTuple<Integer, Double>>> valuesMapper,
        IgniteFunction<V, DoubleStream> labelsMapper,
        IgniteFunction<Integer, Stream<K>> keyMapper,
        Map<Integer, Integer> catFeaturesInfo,
        int featuresCnt, int samplesCnt) {

        cacheName = c.getName();
        this.labelsKeys = labelsKeys;
        this.valuesMapper = valuesMapper;
        this.labelsMapper = labelsMapper;
        this.keyMapper = keyMapper;
        this.catFeaturesInfo = catFeaturesInfo;
        this.samplesCnt = samplesCnt;
        this.featuresCnt = featuresCnt;
    }

    /** {@inheritDoc} */
    @Override public Stream<IgniteBiTuple<Integer, Double>> values(int idx) {
        return cache(Ignition.localIgnite()).getAll(keyMapper.apply(idx).collect(Collectors.toSet())).
            entrySet().
            stream().
            flatMap(ent -> valuesMapper.apply(new CacheEntryImpl<>(ent.getKey(), ent.getValue())));
    }

    /** {@inheritDoc} */
    @Override public double[] labels(Ignite ignite) {
        return labelsKeys.get().map(k -> get(k, ignite)).flatMapToDouble(labelsMapper).toArray();
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Integer> catFeaturesInfo() {
        return catFeaturesInfo;
    }

    /** {@inheritDoc} */
    @Override public int featuresCount() {
        return featuresCnt;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(int idx, Ignite ignite) {
        return ignite.affinity(cacheName).affinityKey(keyMapper.apply(idx));
    }

    /** */
    private V get(K k, Ignite ignite) {
        V res = cache(ignite).localPeek(k);

        if (res == null)
            res = cache(ignite).get(k);

        return res;
    }

    /** */
    private IgniteCache<K, V> cache(Ignite ignite) {
        return ignite.getOrCreateCache(cacheName);
    }
}
