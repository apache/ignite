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

import java.util.Map;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
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
public class DebugCrossValidation<M extends IgniteModel<Vector, Double>, K, V> extends AbstractCrossValidation<M, K, V> {
    /** Upstream map. */
    private Map<K, V> upstreamMap;

    /** Parts. */
    private int parts;

    /**
     * Calculates score by folds.
     */
    @Override public double[] scoreByFolds() {
        double[] locScores;

        locScores = isRunningOnPipeline ? scorePipelineLocally() : scoreLocally();

        return locScores;
    }

    /**
     * Calculate score on pipeline based on local data (upstream map).
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scorePipelineLocally() {
        return scorePipeline(
            predicate -> new LocalDatasetBuilder<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v),
                parts
            )
        );
    }

    /**
     * Computes cross-validated metrics.
     *
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    private double[] scoreLocally() {
        return score(
            predicate -> new LocalDatasetBuilder<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v),
                parts
            )
        );
    }

    /**
     * @param upstreamMap Upstream map.
     */
    public DebugCrossValidation<M, K, V> withUpstreamMap(Map<K, V> upstreamMap) {
        this.upstreamMap = upstreamMap;
        return this;
    }

    /**
     * @param parts Parts.
     */
    public DebugCrossValidation<M, K, V> withAmountOfParts(int parts) {
        this.parts = parts;
        return this;
    }
}
