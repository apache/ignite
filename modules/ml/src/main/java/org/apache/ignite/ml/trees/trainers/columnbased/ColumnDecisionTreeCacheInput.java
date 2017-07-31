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
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Adapter of a given cache to {@see ColumnDecisionTreeCacheInput}
 *
 * @param <K> Class of keys of the cache.
 * @param <V> Class of values of the cache.
 */
public class ColumnDecisionTreeCacheInput<K, V> implements ColumnDecisionTreeInput {
    /** Key of entry containing values which will be mapped to labels. */
    private final K labelsKey;

    /** Count of features. */
    private final int featuresCnt;

    /** Function used for mapping feature index to the cache key. */
    private final IgniteFunction<Integer, K> keyMapper;

    /** Information about which features are categorical in form of feature index -> number of categories. */
    private final Map<Integer, Integer> catFeaturesInfo;

    /** Cache name. */
    private String cacheName;

    /** Count of samples. */
    private final int samplesCnt;

    /** Function used for mapping cache values to stream of tuples. */
    private IgniteFunction<V, Stream<IgniteBiTuple<Integer, Double>>> valsMapper;

    /** Function used for mapping cache value to labels array. */
    private IgniteFunction<V, double[]> labelsMapper;

    /**
     * Constructs input for {@see org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer}.
     *
     * @param c Cache.
     * @param labelsKey Keys of cache entry where labels data is stored.
     * @param valsMapper Function for mapping cache entry to stream used by {@see org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainer}.
     * @param labelsMapper Function used for mapping cache value to labels array.
     * @param keyMapper Function used for mapping feature index to the cache key.
     * @param catFeaturesInfo Information about which features are categorical in form of feature index -> number of
     *      categories.
     * @param featuresCnt Count of features.
     * @param samplesCnt Count of samples.
     */
    // TODO: IGNITE-5724 think about boxing/unboxing
    public ColumnDecisionTreeCacheInput(IgniteCache<K, V> c,
        K labelsKey,
        IgniteFunction<V, Stream<IgniteBiTuple<Integer, Double>>> valsMapper,
        IgniteFunction<V, double[]> labelsMapper,
        IgniteFunction<Integer, K> keyMapper,
        Map<Integer, Integer> catFeaturesInfo,
        int featuresCnt, int samplesCnt) {

        cacheName = c.getName();
        this.labelsKey = labelsKey;
        this.valsMapper = valsMapper;
        this.labelsMapper = labelsMapper;
        this.keyMapper = keyMapper;
        this.catFeaturesInfo = catFeaturesInfo;
        this.samplesCnt = samplesCnt;
        this.featuresCnt = featuresCnt;
    }

    /** {@inheritDoc} */
    @Override public Stream<IgniteBiTuple<Integer, Double>> values(int idx) {
        return valsMapper.apply(get(idx));
    }

    /** {@inheritDoc} */
    @Override public double[] labels() {
        return labelsMapper.apply(get(labelsKey));
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Integer> catFeaturesInfo() {
        return catFeaturesInfo;
    }

    /** {@inheritDoc} */
    @Override public int samplesCount() {
        return samplesCnt;
    }

    /** {@inheritDoc} */
    @Override public int featuresCount() {
        return featuresCnt;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(int idx) {
        Ignite ignite = Ignition.localIgnite();
        return ignite.affinity(cacheName).affinityKey(keyMapper.apply(idx));
    }

    /** */
    private V get(int idx) {
        return get(keyMapper.apply(idx));
    }

    /** */
    private V get(K k) {
        V res = cache().localPeek(k, CachePeekMode.PRIMARY);

        if (res == null)
            res = cache().get(k);
        
        return res;
    }

    /** Get the local ignite instance. */
    private Ignite ignite() {
        return Ignition.localIgnite();
    }

    /** */
    private IgniteCache<K, V> cache() {
        return ignite().getOrCreateCache(cacheName);
    }
}
