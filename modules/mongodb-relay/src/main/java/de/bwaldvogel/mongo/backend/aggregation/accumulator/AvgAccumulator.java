package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import de.bwaldvogel.mongo.backend.NumericUtils;

public class AvgAccumulator extends Accumulator {

    private Number sum = 0;
    private int count;

    public AvgAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (value instanceof Number) {
            sum = NumericUtils.addNumbers(sum, (Number) value);
            count++;
        }
    }

    @Override
    public Number getResult() {
        if (count == 0) {
            return null;
        } else {
            return sum.doubleValue() / count;
        }
    }
}
