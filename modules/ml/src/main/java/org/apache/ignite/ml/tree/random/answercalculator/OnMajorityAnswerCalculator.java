package org.apache.ignite.ml.tree.random.answercalculator;

import org.apache.ignite.ml.tree.random.RandomForestAnswerCalculator;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class OnMajorityAnswerCalculator extends RandomForestAnswerCalculator {
    @Override
    public Double compute(double[] estimations) {
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
