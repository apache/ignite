package org.apache.ignite.ml.tree.random.answercalculator;

import org.apache.ignite.ml.tree.random.RandomForestAnswerCalculator;

import java.util.Arrays;

public class MeanValueAnswerCalculator extends RandomForestAnswerCalculator {
    @Override
    public Double compute(double[] estimations) {
        return Arrays.stream(estimations).reduce(0.0, Double::sum) / estimations.length;
    }
}
