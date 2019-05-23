package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import java.util.Comparator;

import de.bwaldvogel.mongo.backend.Missing;

class ComparingAccumulator extends Accumulator {

    private final Comparator<Object> comparator;
    private Object result;

    ComparingAccumulator(String field, Object expression, Comparator<Object> comparator) {
        super(field, expression);
        this.comparator = comparator;
    }

    @Override
    public void aggregate(Object value) {
        if (Missing.isNullOrMissing(value)) {
            return;
        }
        if (result == null) {
            result = value;
        } else {
            if (comparator.compare(value, result) < 0) {
                result = value;
            }
        }
    }

    @Override
    public Object getResult() {
        return result;
    }
}
