package org.apache.ignite.ml.tree.random;

import java.util.List;
import java.util.function.Function;

public abstract class RandomForestAnswerCalculator implements Function<double[], Double> {
    @Override
    public Double apply(double[] estimations) {
        if(estimations.length == 0)
            throw new IllegalArgumentException("Cannot perform answer for empty estimations list");

        return compute(estimations);
    }

    protected abstract Double compute(double[] estimations);
}
