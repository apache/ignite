package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import java.util.ArrayList;
import java.util.List;

import de.bwaldvogel.mongo.backend.Missing;

public class PushAccumulator extends Accumulator {

    private List<Object> result = new ArrayList<>();

    public PushAccumulator(String field, Object expression) {
        super(field, expression);
    }

    @Override
    public void aggregate(Object value) {
        if (Missing.isNullOrMissing(value)) {
            return;
        }
        result.add(value);
    }

    @Override
    public Object getResult() {
        return result;
    }
}
