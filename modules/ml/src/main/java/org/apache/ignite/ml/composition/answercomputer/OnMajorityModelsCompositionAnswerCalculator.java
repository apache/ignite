package org.apache.ignite.ml.composition.answercomputer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class OnMajorityModelsCompositionAnswerCalculator implements ModelsCompositionAnswerComputer {
    @Override
    public Double apply(double[] estimations) {
        Map<Double, Integer> countersByClass = new HashMap<>();
        for (Double predictedValue : estimations) {
            Integer counterValue = countersByClass.getOrDefault(predictedValue, 0) + 1;
            countersByClass.put(predictedValue, counterValue);
        }

        return countersByClass.entrySet().stream()
                .max(Comparator.comparing(Map.Entry::getValue))
                .get().getKey();
    }
}
