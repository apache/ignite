package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.tree.DecisionTree;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasure;
import org.apache.ignite.ml.tree.random.answercalculator.MeanValueAnswerCalculator;
import org.apache.ignite.ml.tree.random.answercalculator.OnMajorityAnswerCalculator;

import java.util.List;
import java.util.function.Function;

public class RandomForestRegressionTrainer extends RandomForestTrainer<MSEImpurityMeasure> {
    private final MeanValueAnswerCalculator answerCalculator = new MeanValueAnswerCalculator();

    public RandomForestRegressionTrainer(int maxDeep, int minImpurityDecrease,
                                         int treesCount, double learningBatchSize,
                                         int featuresPartSize) {
        super(maxDeep, minImpurityDecrease, treesCount, learningBatchSize, featuresPartSize);
    }

    @Override
    protected DecisionTree<MSEImpurityMeasure> buildDecisionTreeTrainer() {
        return new DecisionTreeRegressionTrainer(maxDeep, minImpurityDecrease);
    }

    @Override
    protected RandomForestAnswerCalculator getAnswerCalculator() {
        return answerCalculator;
    }
}
