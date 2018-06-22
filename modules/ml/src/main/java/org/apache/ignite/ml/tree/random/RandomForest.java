package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.tree.DecisionTreeNode;

import java.util.List;
import java.util.Map;

public class RandomForest implements Model<double[], Double> {
    private final List<RandomTree> trees;
    private final RandomForestAnswerCalculator answerCalculator;

    public RandomForest(List<RandomTree> trees, RandomForestAnswerCalculator answerCalculator) {
        this.trees = trees;
        this.answerCalculator = answerCalculator;
    }

    @Override
    public Double apply(double[] doubles) {
        double[] answers = new double[trees.size()];
        for (int i = 0; i < answers.length; i++)
            answers[i] = trees.get(i).apply(doubles);

        return answerCalculator.apply(answers);
    }

    public static class RandomTree implements Model<double[], Double> {
        private final DecisionTreeNode tree;
        private final Map<Integer, Integer> featuresMapping;

        public RandomTree(DecisionTreeNode tree, Map<Integer, Integer> featuresMapping) {
            this.tree = tree;
            this.featuresMapping = featuresMapping;
        }

        @Override
        public Double apply(double[] features) {
            double[] newFeatures = new double[features.length];
            for (int i = 0; i < newFeatures.length; i++)
                newFeatures[i] = features[featuresMapping.get(i)];
            return tree.apply(newFeatures);
        }
    }
}
