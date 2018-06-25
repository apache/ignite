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

package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.composition.answercomputer.PredictionsAggregator;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.composition.answercomputer.MeanValuePredictionsAggregator;

import java.util.concurrent.ExecutorService;

public class RandomForestRegressionTrainer extends RandomForestTrainer {
    public RandomForestRegressionTrainer(PredictionsAggregator predictionsAggregator,
                                         int featureVectorSize, int maximumFeaturesCountPerModel,
                                         int countOfModels, double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease,
                                         ExecutorService threadPool) {
        super(predictionsAggregator,
                featureVectorSize,
                maximumFeaturesCountPerModel,
                countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease, threadPool);
    }

    public RandomForestRegressionTrainer(int featureVectorSize,
                                         int maximumFeaturesCountPerModel,
                                         int countOfModels,
                                         double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease,
                                         ExecutorService threadPool) {
        this(new MeanValuePredictionsAggregator(),
                featureVectorSize,
                maximumFeaturesCountPerModel,
                countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease, threadPool);
    }

    public RandomForestRegressionTrainer(int featureVectorSize,
                                         int maximumFeaturesCountPerModel,
                                         int countOfModels,
                                         double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease) {
        this(new MeanValuePredictionsAggregator(),
                featureVectorSize,
                maximumFeaturesCountPerModel,
                countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease, null);
    }

    @Override
    protected DatasetTrainer<DecisionTreeNode, Double> buildDatasetTrainerForModel() {
        return new DecisionTreeRegressionTrainer(maxDeep, minImpurityDecrease);
    }
}
