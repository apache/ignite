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

package org.apache.ignite.ml.tree.randomforest;

import org.apache.ignite.ml.composition.BaggingModelTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;

/**
 * Abstract random forest trainer.
 */
public abstract class RandomForestTrainer extends BaggingModelTrainer {
    /** Max decision tree deep. */
    protected final int maxDeep;
    /** Min impurity decrease. */
    protected final double minImpurityDecrease;

    /**
     * Constructs new instance of BaggingModelTrainer.
     *
     * @param predictionsAggregator Predictions aggregator.
     * @param featureVectorSize Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize Ensemble size.
     * @param samplePartSizePerMdl Size of sample part in percent to train one model.
     * @param maxDeep Max decision tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public RandomForestTrainer(PredictionsAggregator predictionsAggregator,
        int featureVectorSize,
        int maximumFeaturesCntPerMdl,
        int ensembleSize,
        double samplePartSizePerMdl,
        int maxDeep,
        double minImpurityDecrease) {

        super(predictionsAggregator, featureVectorSize, maximumFeaturesCntPerMdl,
            ensembleSize, samplePartSizePerMdl);

        this.maxDeep = maxDeep;
        this.minImpurityDecrease = minImpurityDecrease;
    }
}
