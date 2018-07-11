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

import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Random forest classifier trainer.
 */
public class RandomForestClassifierTrainer extends RandomForestTrainer {
    /**
     * Constructs new instance of RandomForestClassifierTrainer.
     *
     * @param predictionsAggregator Predictions aggregator.
     * @param featureVectorSize Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize Ensemble size.
     * @param samplePartSizePerMdl Size of sample part in percent to train one model.
     * @param maxDeep Max decision tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public RandomForestClassifierTrainer(PredictionsAggregator predictionsAggregator,
        int featureVectorSize,
        int maximumFeaturesCntPerMdl,
        int ensembleSize,
        double samplePartSizePerMdl,
        int maxDeep,
        double minImpurityDecrease) {

        super(predictionsAggregator, featureVectorSize, maximumFeaturesCntPerMdl,
            ensembleSize, samplePartSizePerMdl, maxDeep, minImpurityDecrease);
    }

    /**
     * Constructs new instance of RandomForestClassifierTrainer.
     *
     * @param featureVectorSize Feature vector size.
     * @param maximumFeaturesCntPerMdl Number of features to draw from original features vector to train each model.
     * @param ensembleSize Ensemble size.
     * @param samplePartSizePerMdl Size of sample part in percent to train one model.
     * @param maxDeep Max decision tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public RandomForestClassifierTrainer(int featureVectorSize,
        int maximumFeaturesCntPerMdl,
        int ensembleSize,
        double samplePartSizePerMdl,
        int maxDeep, double minImpurityDecrease) {

        this(new OnMajorityPredictionsAggregator(), featureVectorSize, maximumFeaturesCntPerMdl,
            ensembleSize, samplePartSizePerMdl, maxDeep, minImpurityDecrease);
    }

    /** {@inheritDoc} */
    @Override protected DatasetTrainer<DecisionTreeNode, Double> buildDatasetTrainerForModel() {
        return new DecisionTreeClassificationTrainer(maxDeep, minImpurityDecrease);
    }
}
