package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.tree.DecisionTree;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasure;
import org.apache.ignite.ml.tree.random.answercalculator.OnMajorityAnswerCalculator;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class RandomForestClassifierTrainer extends RandomForestTrainer<GiniImpurityMeasure> {
    private final RandomForestAnswerCalculator answerCalculator = new OnMajorityAnswerCalculator();

    public RandomForestClassifierTrainer(int maxDeep, int minImpurityDecrease,
                                         int treesCount, double learningBatchSize,
                                         int featuresPartSize) {
        super(maxDeep, minImpurityDecrease, treesCount, learningBatchSize, featuresPartSize);
    }

    @Override
    protected DecisionTree<GiniImpurityMeasure> buildDecisionTreeTrainer() {
        return new DecisionTreeClassificationTrainer(maxDeep, minImpurityDecrease);
    }

    @Override
    protected RandomForestAnswerCalculator getAnswerCalculator() {
        return answerCalculator;
    }
}
