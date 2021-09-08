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

package org.apache.ignite.ml.selection.cv;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Cross validation score calculator. Cross validation is an approach that allows to avoid overfitting that is made the
 * following way: the training set is split into k smaller sets. The following procedure is followed for each of the k
 * “folds”:
 * <ul>
 * <li>A model is trained using k-1 of the folds as training data;</li>
 * <li>the resulting model is validated on the remaining part of the data (i.e., it is used as a test set to compute
 * a performance measure such as accuracy).</li>
 * </ul>
 *
 * @param <M> Type of model.
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CrossValidation<M extends IgniteModel<Vector, Double>, K, V> extends AbstractCrossValidation<M, K, V> {
    /** Ignite. */
    private Ignite ignite;

    /** Upstream cache. */
    private IgniteCache<K, V> upstreamCache;

    /**
     * Calculates score by folds.
     */
    @Override public double[] scoreByFolds() {
        double[] locScores;

        locScores = isRunningOnPipeline ? scorePipelineOnIgnite() : scoreOnIgnite();

        return locScores;
    }

    /**
     * Calculate score on pipeline based on Ignite data (upstream cache).
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scorePipelineOnIgnite() {
        return scorePipeline(
            predicate -> new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v)
            )
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scoreOnIgnite() {
        return score(
            predicate -> new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v)
            )
        );
    }

    /**
     * @param ignite Ignite.
     */
    public CrossValidation<M, K, V> withIgnite(Ignite ignite) {
        this.ignite = ignite;
        return this;
    }

    /**
     * @param upstreamCache Upstream cache.
     */
    public CrossValidation<M, K, V> withUpstreamCache(IgniteCache<K, V> upstreamCache) {
        this.upstreamCache = upstreamCache;
        return this;
    }
}
