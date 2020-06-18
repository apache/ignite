package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.NumericUtils;

public class SumAccumulator extends Accumulator {

    private Number sum = 0;

    public SumAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value instanceof Number) {
            sum = NumericUtils.addNumbers(sum, (Number) value);
        }
    }

    @Override
    public Number getResult() {
        return sum;
    }
}
