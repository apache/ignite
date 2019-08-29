package de.bwaldvogel.mongo.backend.aggregation.accumulator;

public class MinAccumulator extends ComparingAccumulator {

    public MinAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    protected boolean shouldTakeValue(int comparisonResult) {
        return comparisonResult < 0;
    }

}
