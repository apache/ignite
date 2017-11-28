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
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Adapter for column decision tree trainer for bi-indexed cache.
 */
public class BiIndexedCacheColumnDecisionTreeTrainerInput extends CacheColumnDecisionTreeTrainerInput<BiIndex, Double> {
    /**
     * Construct an input for {@link ColumnDecisionTreeTrainer}.
     *
     * @param cache Bi-indexed cache.
     * @param catFeaturesInfo Information about categorical feature in the form (feature index -> number of
     * categories).
     * @param samplesCnt Count of samples.
     * @param featuresCnt Count of features.
     */
    public BiIndexedCacheColumnDecisionTreeTrainerInput(IgniteCache<BiIndex, Double> cache,
        Map<Integer, Integer> catFeaturesInfo, int samplesCnt, int featuresCnt) {
        super(cache,
            () -> IntStream.range(0, samplesCnt).mapToObj(s -> new BiIndex(s, featuresCnt)),
            e -> Stream.of(new IgniteBiTuple<>(e.getKey().row(), e.getValue())),
            DoubleStream::of,
            fIdx -> IntStream.range(0, samplesCnt).mapToObj(s -> new BiIndex(s, fIdx)),
            catFeaturesInfo,
            featuresCnt,
            samplesCnt);
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(int idx, Ignite ignite) {
        return idx;
    }
}
