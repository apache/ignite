package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.composition.BaggingClassifierTrainer;
import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

public abstract class RandomForestTrainer extends BaggingClassifierTrainer<DecisionTreeNode> {
    protected final int maxDeep;
    protected final double minImpurityDecrease;

    public RandomForestTrainer(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer,
                               int maximumFeaturesCountPerModel,
                               int countOfModels,
                               double samplePartSizePerModel,
                               int maxDeep,
                               double minImpurityDecrease) {
        super(modelsCompositionAnswerComputer, maximumFeaturesCountPerModel, countOfModels, samplePartSizePerModel);
        this.maxDeep = maxDeep;
        this.minImpurityDecrease = minImpurityDecrease;
    }

    @Override
    protected int getFeaturesVectorSize(DecisionTreeNode model) {
        return model.getFeaturesCount();
    }
}
