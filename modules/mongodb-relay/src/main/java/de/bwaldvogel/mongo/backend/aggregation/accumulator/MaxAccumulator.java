package de.bwaldvogel.mongo.backend.aggregation.accumulator;

public class MaxAccumulator extends ComparingAccumulator {

    public MaxAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    protected boolean shouldTakeValue(int comparisonResult) {
        return comparisonResult > 0;
    }

}
