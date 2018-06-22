package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.composition.answercomputer.OnMajorityModelsCompositionAnswerCalculator;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

public class RandomForestClassifierTrainer extends RandomForestTrainer {
    public RandomForestClassifierTrainer(int maximumFeaturesCountPerModel,
                                         int countOfModels, double samplePartSizePerModel,
                                         int maxDeep, double minImpurityDecrease) {
        super(new OnMajorityModelsCompositionAnswerCalculator(),
                maximumFeaturesCountPerModel,
                countOfModels,
                samplePartSizePerModel,
                maxDeep,
                minImpurityDecrease);
    }

    @Override
    protected DatasetTrainer<DecisionTreeNode, Double> buildDatasetTrainerForModel() {
        return new DecisionTreeClassificationTrainer(maxDeep, minImpurityDecrease);
    }
}
