package org.apache.ignite.ml.composition.answercomputer;

import java.util.Arrays;

public class MeanValueModelsCompositionAnswerCalculator implements ModelsCompositionAnswerComputer {
    @Override
    public Double apply(double[] estimations) {
        return Arrays.stream(estimations).reduce(0.0, Double::sum) / estimations.length;
    }
}
