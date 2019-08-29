package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import java.util.Comparator;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.ValueComparator;

abstract class ComparingAccumulator extends Accumulator {

    private final Comparator<Object> comparator = ValueComparator.ascWithoutListHandling();
    private Object result;

    ComparingAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (Missing.isNullOrMissing(value)) {
            return;
        }
        if (result == null) {
            result = value;
        } else {
            int comparisonResult = comparator.compare(value, result);
            if (shouldTakeValue(comparisonResult)) {
                result = value;
            }
        }
    }

    protected abstract boolean shouldTakeValue(int comparisonResult);

    @Override
    public Object getResult() {
        return result;
    }
}
