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

import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.composition.answercomputer.MeanValueModelsCompositionAnswerCalculator;

public class RandomForestRegressionTrainer extends RandomForestTrainer {
    public RandomForestRegressionTrainer(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer,
                                         int maximumFeaturesCountPerModel, int featureVectorSize,
                                         int countOfModels, double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease) {
        super(modelsCompositionAnswerComputer, maximumFeaturesCountPerModel,
                featureVectorSize, countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease);
    }

    public RandomForestRegressionTrainer(int featureVectorSize,
                                         int maximumFeaturesCountPerModel,
                                         int countOfModels,
                                         double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease) {
        super(new MeanValueModelsCompositionAnswerCalculator(),
                maximumFeaturesCountPerModel, featureVectorSize,
                countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease);
    }

    @Override
    protected DatasetTrainer<DecisionTreeNode, Double> buildDatasetTrainerForModel() {
        return new DecisionTreeRegressionTrainer(maxDeep, minImpurityDecrease);
    }
}
