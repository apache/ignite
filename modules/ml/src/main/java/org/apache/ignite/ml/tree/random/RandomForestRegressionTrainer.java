package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.composition.answercomputer.MeanValueModelsCompositionAnswerCalculator;

public class RandomForestRegressionTrainer extends RandomForestTrainer {
    public RandomForestRegressionTrainer(int maximumFeaturesCountPerModel,
                                         int countOfModels,
                                         double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease) {
        super(new MeanValueModelsCompositionAnswerCalculator(),
                maximumFeaturesCountPerModel,
                countOfModels,
                samplePartSizePerModel,
                maxDeep, minImpurityDecrease);
    }

    @Override
    protected DatasetTrainer<DecisionTreeNode, Double> buildDatasetTrainerForModel() {
        return new DecisionTreeRegressionTrainer(maxDeep, minImpurityDecrease);
    }
}
