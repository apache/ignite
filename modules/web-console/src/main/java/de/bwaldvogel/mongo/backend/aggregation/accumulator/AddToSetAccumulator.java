package de.bwaldvogel.mongo.backend.aggregation.accumulator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import de.bwaldvogel.mongo.backend.Missing;

public class AddToSetAccumulator extends Accumulator {

    private Set<Object> result = new LinkedHashSet<>();

    public AddToSetAccumulator(String field, Object expression) {
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
    public List<Object> getResult() {
        List<Object> list = new ArrayList<>(result);
        Collections.reverse(list);
        return list;
    }
}
